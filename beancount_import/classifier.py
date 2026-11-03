"""Nearest-neighbor account classifier.

An alternative to the decision tree for predicting unknown accounts, selected
with the `classifier` option (`nearest_neighbor`).

Every training example (a `PredictionInput` plus the account the user chose) is
kept.  To predict, the example is compared against all stored examples of the
same kind using TF-IDF-weighted cosine similarity over normalized tokens of the
metadata values, and the k most similar examples vote, weighted by
`similarity ** power` and by an exponential decay on their age.

Two properties matter for interactive use:

- Learning is incremental: `add` makes an accepted transaction available to the
  very next prediction, with no retraining step.
- Recent choices win: if you start booking a merchant differently, the new
  account takes over after a few examples rather than being outvoted by years
  of history.
"""

import collections
import datetime
import math
import re
from decimal import Decimal
from typing import Callable, Counter, Dict, Hashable, List, NamedTuple, Optional, Tuple

import numpy as np
import scipy.sparse
from sklearn.feature_extraction import FeatureHasher

from .training import PredictionInput

# Keys whose values duplicate other keys (the OFX source sets `desc` to the
# name or memo).
_REDUNDANT_KEYS = frozenset(['desc'])

_SPLIT_RE = re.compile(r'[\s*#/,;:()\[\]"]+')
# Some card feeds (AMEX) prefix the memo with the cardholder name, account
# suffix and a per-transaction reference, e.g.
# "JANE DOE-12345-xA1B2C3D4 squareup.com/receipts AplPay CORNER MARKET Anytown".
# Left in, every such memo looks alike; the merchant is what follows.
_CARDHOLDER_PREFIX_RE = re.compile(
    r'^[A-Z][A-Z .\'-]+-\d{4,6}-\S+\s*(?:squareup\.com/receipts\s*)?')


def _strip_cardholder_prefix(value: str) -> str:
    stripped = _CARDHOLDER_PREFIX_RE.sub('', value)
    return stripped or value
_DIGIT_RE = re.compile(r'\d')
_NUMBER_RE = re.compile(r'[^0-9]+')


def _words(value: str) -> List[str]:
    """Lower-cases and tokenizes; tokens containing digits collapse to '#'."""
    out = []
    for w in _SPLIT_RE.split(value.lower()):
        w = w.strip("-.'")
        if not w:
            continue
        if _DIGIT_RE.search(w) and (w.replace('.', '').replace('-', '').isdigit()
                                    or sum(c.isdigit() for c in w) > 2):
            w = '#'
        out.append(w)
    return out


def get_features(example: PredictionInput) -> Dict[str, float]:
    """Sparse features for similarity.

    Besides word unigrams/bigrams, the full normalized value (alone and paired
    with the source account) gives exact merchant matches a strong signal, and
    short digit runs (e.g. the last 4 digits in "PAYMENT TO CARD ENDING IN
    1234") are kept because they often identify the counterparty.
    """
    f = {}  # type: Dict[str, float]
    account = example.source_account
    f['acct=' + account] = 1
    number = example.amount.number if example.amount is not None else None
    if number is None:
        number = Decimal(0)
    sign = 'neg' if number < 0 else 'pos'
    magnitude = int(math.floor(math.log2(abs(float(number)) + 1)))
    f['sign=' + sign] = 1
    f['amt=%s%d' % (sign, magnitude)] = 1
    f['acctsign=%s|%s' % (account, sign)] = 1
    for key, values in example.key_value_pairs.items():
        if key in _REDUNDANT_KEYS:
            continue
        if isinstance(values, str):
            values = (values, )
        f['has=' + key] = 1
        for value in values:
            value = _strip_cardholder_prefix(value)
            words = _words(value)
            full = ' '.join(words)
            f['full=%s:%s' % (key, full)] = 1
            f['full+acct=%s:%s:%s' % (account, key, full)] = 1
            for w in words:
                f['w=%s:%s' % (key, w)] = 1
                f['w=' + w] = 0.5
            for n in re.split(_NUMBER_RE, value):
                if 2 <= len(n) <= 6:
                    f['n=%s:%s' % (key, n)] = 1
            for a, b in zip(words, words[1:]):
                f['b=%s:%s %s' % (key, a, b)] = 1
            for n in (1, 2, 3):
                if len(words) >= n:
                    f['pre%d=%s:%s' % (n, key, ' '.join(words[:n]))] = 1
    return f


_ALPHA_RE = re.compile(r'[a-z]{3,}')

# Transaction-level keys from other sources are also mixed into
# key_value_pairs; these carry no merchant identity.
_NON_TEXT_KEYS = frozenset(['desc', 'amazon_account', 'ofx_type'])


def get_encoder_features(example: PredictionInput) -> Dict[str, float]:
    """`get_features` plus character 3/4-grams of alphabetic words, which let
    the encoder generalize across merchants sharing word pieces."""
    f = get_features(example)
    for key, values in example.key_value_pairs.items():
        if key in _NON_TEXT_KEYS:
            continue
        if isinstance(values, str):
            values = (values, )
        for value in values:
            for w in _ALPHA_RE.findall(_strip_cardholder_prefix(value).lower()):
                w = '<' + w + '>'
                for n in (3, 4):
                    for i in range(len(w) - n + 1):
                        f['c=' + w[i:i + n]] = 0.3
    return f


def _merchant_key(example: PredictionInput) -> str:
    parts = []  # type: List[str]
    for key, values in sorted(example.key_value_pairs.items()):
        if key in _NON_TEXT_KEYS:
            continue
        if isinstance(values, str):
            values = (values, )
        parts.extend(' '.join(_words(v)) for v in values)
    return ' '.join(parts)[:40]


class TripletEncoder(object):
    """Linear Siamese encoder e(x) = normalize(x W) over sparse TF-IDF features.

    Trained with batch-hard triplet loss: each batch holds
    `examples_per_label` examples for each of `labels_per_batch` accounts,
    drawn from distinct merchants where possible, so that the encoder learns
    what merchants booked to the same account have in common rather than
    memorizing exact matches (the TF-IDF similarity already does that).
    Optimized with Adagrad on the rows of W touched by each batch.
    """

    def __init__(self,
                 dim: int = 64,
                 margin: float = 0.3,
                 labels_per_batch: int = 48,
                 examples_per_label: int = 4,
                 steps: int = 2000,
                 learning_rate: float = 0.2,
                 min_df: int = 2,
                 seed: int = 0) -> None:
        self.dim = dim
        self.margin = margin
        self.labels_per_batch = labels_per_batch
        self.examples_per_label = examples_per_label
        self.steps = steps
        self.learning_rate = learning_rate
        self.min_df = min_df
        self.seed = seed
        self.vocabulary = {}  # type: Dict[str, int]
        self.idf = None  # type: Optional[np.ndarray]
        self.W = None  # type: Optional[np.ndarray]

    def _vectorize(self, feature_dicts: List[Dict[str, float]]
                   ) -> scipy.sparse.csr_matrix:
        vocabulary = self.vocabulary
        indptr = [0]
        indices = []  # type: List[int]
        data = []  # type: List[float]
        for f in feature_dicts:
            for name, value in f.items():
                j = vocabulary.get(name)
                if j is not None:
                    indices.append(j)
                    data.append(value)
            indptr.append(len(indices))
        X = scipy.sparse.csr_matrix(
            (np.array(data, dtype=np.float32), np.array(indices, dtype=np.int64),
             np.array(indptr, dtype=np.int64)),
            shape=(len(feature_dicts), len(vocabulary)))
        X.sum_duplicates()
        X = X.multiply(self.idf).tocsr()
        norms = np.sqrt(np.asarray(X.multiply(X).sum(axis=1)).ravel())
        norms[norms == 0] = 1
        return (scipy.sparse.diags(1 / norms) @ X).tocsr()

    def fit(self, examples: List[PredictionInput], targets: List[str]) -> None:
        rng = np.random.default_rng(self.seed)
        feature_dicts = [get_encoder_features(e) for e in examples]
        df = collections.Counter()  # type: Counter[str]
        for f in feature_dicts:
            df.update(f.keys())
        names = sorted(name for name, c in df.items() if c >= self.min_df)
        self.vocabulary = {name: j for j, name in enumerate(names)}
        n = len(examples)
        self.idf = (np.log((1 + n) / (1 + np.array([df[x] for x in names], dtype=np.float32))) + 1).astype(np.float32)
        X = self._vectorize(feature_dicts)
        D = len(names)
        W = rng.normal(0, 1.0 / math.sqrt(self.dim), (D, self.dim)).astype(np.float32)
        G = np.full(D, 1e-6, dtype=np.float32)

        by_label = collections.defaultdict(lambda: collections.defaultdict(list))  # type: Dict[str, Dict[str, List[int]]]
        for i, (e, t) in enumerate(zip(examples, targets)):
            by_label[t][_merchant_key(e)].append(i)
        labels = [t for t, ms in by_label.items() if sum(map(len, ms.values())) >= 2]
        if len(labels) < 2:
            self.W = W
            return
        label_merchants = [list(by_label[t].values()) for t in labels]
        p = np.sqrt(np.array([sum(map(len, ms)) for ms in label_merchants], dtype=np.float64))
        p /= p.sum()
        K = self.examples_per_label
        for _ in range(self.steps):
            chosen = rng.choice(len(labels), size=min(self.labels_per_batch, len(labels)), replace=False, p=p)
            idx = []  # type: List[int]
            lab = []  # type: List[int]
            for li in chosen:
                ms = label_merchants[li]
                order = rng.permutation(len(ms))[:K]
                pick = [ms[o][rng.integers(len(ms[o]))] for o in order]
                while len(pick) < K:
                    m = ms[rng.integers(len(ms))]
                    pick.append(m[rng.integers(len(m))])
                idx.extend(pick)
                lab.extend([li] * K)
            lab_arr = np.array(lab)
            Xb = X[np.array(idx)]
            cols = np.unique(Xb.indices)
            Xs = Xb[:, cols]
            Z = np.asarray(Xs @ W[cols])
            nz = np.linalg.norm(Z, axis=1, keepdims=True) + 1e-8
            E = Z / nz
            S = E @ E.T
            same = lab_arr[:, None] == lab_arr[None, :]
            np.fill_diagonal(same, False)
            a = np.arange(len(idx))
            pos = np.where(same, S, np.inf).argmin(1)
            neg = np.where(lab_arr[:, None] != lab_arr[None, :], S, -np.inf).argmax(1)
            active = self.margin - S[a, pos] + S[a, neg] > 0
            if not active.any():
                continue
            aa = a[active]
            gE = np.zeros_like(E)
            np.add.at(gE, aa, E[neg[aa]] - E[pos[aa]])
            np.add.at(gE, pos[aa], -E[aa])
            np.add.at(gE, neg[aa], E[aa])
            gE /= len(idx)
            gZ = (gE - (gE * E).sum(1, keepdims=True) * E) / nz
            gW = np.asarray(Xs.T @ gZ)
            G[cols] += (gW**2).mean(1)
            W[cols] -= self.learning_rate * gW / np.sqrt(G[cols])[:, None]
        self.W = W

    def transform(self, examples: List[PredictionInput]) -> np.ndarray:
        X = self._vectorize([get_encoder_features(e) for e in examples])
        Z = np.asarray(X @ self.W)
        return Z / (np.linalg.norm(Z, axis=1, keepdims=True) + 1e-8)


def _example_key(example: PredictionInput) -> Hashable:
    """Everything `get_features` and `get_encoder_features` depend on."""
    return (example.source_account, example.amount,
            tuple(sorted((k, v if isinstance(v, str) else tuple(v))
                         for k, v in example.key_value_pairs.items())))


# Vectors computed by a previous classifier (see `reuse_vectors_of`):
# `row_of` maps an example key to its row in `hashed` and (if not None)
# `embeddings`, the latter computed by `encoder`.
_ReusedVectors = NamedTuple('_ReusedVectors', [
    ('row_of', Dict[Hashable, int]),
    ('hashed', scipy.sparse.csr_matrix),
    ('encoder', Optional['TripletEncoder']),
    ('embeddings', Optional[np.ndarray]),
])


class NearestNeighborClassifier(object):
    def __init__(self,
                 k: int = 10,
                 power: float = 4.0,
                 half_life_days: float = 365.0,
                 alpha: float = 0.5,
                 n_features: int = 2**20) -> None:
        """`alpha` weights TF-IDF similarity against encoder similarity when
        an encoder is set (1 = TF-IDF only)."""
        self.k = k
        self.alpha = alpha
        self.encoder = None  # type: Optional[TripletEncoder]
        self._examples = []  # type: List[PredictionInput]
        self._embeddings = None  # type: Optional[np.ndarray]
        self.power = power
        self.half_life_days = half_life_days
        self._hasher = FeatureHasher(
            n_features=n_features, input_type='dict', alternate_sign=False)
        # Hashed features of the first `_num_hashed` examples.
        self._blocks = []  # type: List[scipy.sparse.csr_matrix]
        self._num_hashed = 0
        self._reused = None  # type: Optional[_ReusedVectors]
        self._targets = []  # type: List[str]
        self._ordinals = []  # type: List[int]
        self._kinds = []  # type: List[int]
        self._kind_ids = {}  # type: Dict[Hashable, int]
        self._matrix = None  # type: Optional[scipy.sparse.csr_matrix]
        self._idf = None  # type: Optional[np.ndarray]

    def __len__(self) -> int:
        return len(self._targets)

    def add(self, example: PredictionInput, target_account: str,
            kind: Hashable = None) -> None:
        self._examples.append(example)
        self._targets.append(target_account)
        self._ordinals.append(example.date.toordinal())
        self._kinds.append(self._kind_ids.setdefault(kind, len(self._kind_ids)))
        self._matrix = None

    def reuse_vectors_of(self, previous: 'NearestNeighborClassifier') -> None:
        """Lets the next index build take the feature vectors (and, if the
        same encoder is set, the embeddings) of examples `previous` already
        has, rather than computing them again.  Used when the journal is
        reloaded, which changes few of its examples."""
        if not previous._blocks:
            return
        hashed = previous._blocks[0]
        embeddings = previous._embeddings
        n = hashed.shape[0]
        if embeddings is not None:
            n = min(n, len(embeddings))
        row_of = {
            _example_key(e): j
            for j, e in enumerate(previous._examples[:n])
        }
        self._reused = _ReusedVectors(
            row_of=row_of,
            hashed=hashed,
            encoder=previous.encoder,
            embeddings=embeddings)

    def _rows(self, examples: List[PredictionInput], reused,
              compute: Callable, vstack: Callable):
        """`compute(examples)`, except that rows of examples known to
        `self._reused` are taken from `reused` instead."""
        if self._reused is None or reused is None:
            return compute(examples)
        row_of = self._reused.row_of
        reused_rows = []  # type: List[int]
        order = []  # type: List[int]
        new = []  # type: List[int]
        for i, e in enumerate(examples):
            j = row_of.get(_example_key(e))
            if j is None:
                new.append(i)
            else:
                order.append(i)
                reused_rows.append(j)
        if not reused_rows:
            return compute(examples)
        parts = [reused[np.array(reused_rows)]]
        if new:
            parts.append(compute([examples[i] for i in new]))
        order.extend(new)
        position = np.empty(len(order), dtype=np.int64)
        position[order] = np.arange(len(order))
        return vstack(parts)[position]

    def _get_matrix(self) -> Tuple[scipy.sparse.csr_matrix, np.ndarray]:
        if self._matrix is None:
            if self._num_hashed < len(self._examples):
                self._blocks.append(self._rows(
                    self._examples[self._num_hashed:],
                    self._reused and self._reused.hashed,
                    lambda examples: self._hasher.transform(
                        [get_features(e) for e in examples]),
                    lambda parts: scipy.sparse.vstack(parts).tocsr()))
                self._num_hashed = len(self._examples)
            raw = scipy.sparse.vstack(self._blocks).tocsr()
            self._blocks = [raw]
            # Smoothed IDF, as sklearn's TfidfTransformer.  Feature values are
            # already 0/0.5/1, so no TF scaling is needed.
            df = np.bincount(raw.indices, minlength=raw.shape[1])
            n = raw.shape[0]
            self._idf = np.log((1 + n) / (1 + df)) + 1
            m = raw.multiply(self._idf).tocsr()
            norms = np.sqrt(np.asarray(m.multiply(m).sum(axis=1)).ravel())
            norms[norms == 0] = 1
            self._matrix = scipy.sparse.diags(1 / norms) @ m
            self._arrays = (np.array(self._targets), np.array(self._ordinals),
                            np.array(self._kinds))
            if self.encoder is not None:
                previous = self._embeddings
                done = 0 if previous is None else len(previous)
                reused = self._reused
                new = self._rows(
                    self._examples[done:],
                    reused.embeddings if reused is not None
                    and reused.encoder is self.encoder else None,
                    self.encoder.transform, np.vstack)
                self._embeddings = new if previous is None else np.vstack(
                    [previous, new])
            # Examples added later are new.
            self._reused = None
        assert self._idf is not None
        return self._matrix, self._idf

    def prepare(self) -> None:
        """Builds the index now rather than on the next prediction."""
        if self._targets:
            self._get_matrix()

    def fit_encoder(self, **kwargs) -> None:
        """Trains a `TripletEncoder` on all examples added so far."""
        encoder = TripletEncoder(**kwargs)
        encoder.fit(self._examples, self._targets)
        self.set_encoder(encoder)

    def set_encoder(self, encoder: Optional[TripletEncoder]) -> None:
        self.encoder = encoder
        self._embeddings = None
        self._matrix = None

    def _vectorize(self, example: PredictionInput) -> scipy.sparse.csr_matrix:
        _, idf = self._get_matrix()
        v = self._hasher.transform([get_features(example)]).multiply(idf).tocsr()
        norm = math.sqrt(v.multiply(v).sum())
        return v / norm if norm else v

    def predict_scores(self, example: PredictionInput,
                       kind: Hashable = None) -> Dict[str, float]:
        """Returns account -> normalized vote weight (sums to 1), or {}."""
        if not self._targets:
            return {}
        matrix, _ = self._get_matrix()
        targets, ordinals, kinds = self._arrays
        sims = (matrix @ self._vectorize(example).T).toarray().ravel()
        if self.encoder is not None:
            e = self.encoder.transform([example])[0]
            sims = self.alpha * sims + (1 - self.alpha) * np.maximum(
                self._embeddings @ e, 0)
        candidates = np.flatnonzero(sims > 0)
        if example.source_account:
            # The unknown posting is opposite the source posting; predicting
            # the source account itself is never right.
            candidates = candidates[targets[candidates] != example.source_account]
        same_kind = candidates[kinds[candidates] == self._kind_ids.get(kind, -1)]
        if len(same_kind):
            candidates = same_kind
        if not len(candidates):
            return {}
        top = candidates[np.argsort(-sims[candidates])[:self.k]]
        ordinal = example.date.toordinal()
        scores = collections.defaultdict(float)  # type: Dict[str, float]
        for i in top:
            age = max(0, ordinal - int(ordinals[i]))
            scores[self._targets[i]] += float(sims[i])**self.power * 0.5**(
                age / self.half_life_days)
        total = sum(scores.values())
        return {a: s / total for a, s in scores.items()} if total else {}

    def predict(self, example: PredictionInput,
                kind: Hashable = None) -> Optional[str]:
        scores = self.predict_scores(example, kind)
        if not scores:
            return None
        return max(scores, key=lambda account: scores[account])
