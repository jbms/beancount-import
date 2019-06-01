import { EventEmitter } from "fbemitter";
export type JournalError = [
  string,
  string,
  {
    filename: string;
    lineno: number;
  }
];

export interface UnclearedPosting {
  transaction: BeancountEntry;
  posting: BeancountEntry;
  transaction_formatted: string;
}

export interface InvalidReference {
  num_extras: number;
  source: string;
  transaction_posting_pairs: {
    transaction: BeancountEntry;
    posting: BeancountEntry | null | undefined;
    transaction_formatted: string;
    posting_formatted: string | null | undefined;
  }[];
}

/**
 * uniqueName, accountName, groupNumber, originalName, predictedName
 */
export type SubstitutedAccount = [string, string, number, string, string];

/**
 * originalStartLine, originalEndLine
 */
export type LineRange = [number, number];

export enum LineChangeType {
  delete = -1,
  context = 0,
  insert = 1
}

/**
 * changeType, line
 */
export type LineChange = [LineChangeType, string];

export type LineChangeSet = [LineRange, LineChange[]];

/**
 * filename, lineChangeSets
 */
export type FileChangeSet = [string, LineChangeSet[]];

export interface BeancountMeta {
  lineno?: number;
  filename?: string;
  [key: string]: string | number | undefined;
}

export interface BeancountPosting {
  account: string;
  units: any;
  cost: any;
  price: any;
}

export interface BeancountDirectiveBase {
  date: string;
  meta?: BeancountMeta;
}

export interface BeancountTransaction extends BeancountDirectiveBase {
  tags: string[] | null;
  links: string[] | null;
  flag: string;
  payee: string;
  narration: string;
  postings: BeancountPosting[];
}

export interface BeancountOpen extends BeancountDirectiveBase {
  account: string;
  currencies: string[] | null;
}

export type BeancountEntry = BeancountTransaction | BeancountOpen;

export interface TransactionProperties {
  narration: string;
  payee: string | null;
  tags: string[];
  links: string[];
}

export interface AssociatedEntryData {
  meta?: [string, any] | null;
  link?: string | null;
  description: string;
  type: string;
  path: string;
}

export interface Candidate {
  used_transaction_ids: number[];
  substituted_accounts: SubstitutedAccount[];
  change_sets: FileChangeSet[];
  original_transaction_properties?: TransactionProperties;
  new_entries: BeancountEntry[];
  associated_data: AssociatedEntryData[];
}

export interface UsedTransaction {
  formatted: string;
  entry: BeancountEntry;
  pending_index: number;
  source: string | null;
  info: any;
}

export interface Candidates {
  candidates: Candidate[];
  date: string;
  amount: string;
  used_transactions: UsedTransaction[];
}

export interface PendingEntrySourceInfo {
  type?: string;
  filename?: string;
  line?: number;
}

export interface PendingEntry {
  date: string;
  formatted: string;
  id: string;
  source: string | null;
  info?: PendingEntrySourceInfo;
  entries: BeancountEntry[];
}

export type GenerationAndCount = [number, number];

export interface ServerState {
  opened: boolean;
  closed: boolean;
  message: string;
  pending_index: number | null;
  skip_ids: string[];
  journal_filenames: string[];
  main_journal_path: string;
  accounts: string[];
  candidates: Candidates;
  candidates_generation: number;
  action_sounds: [string, string][];
  pending: GenerationAndCount | null;
  errors: GenerationAndCount | null;
  uncleared: GenerationAndCount | null;
  invalid: GenerationAndCount | null;
}

interface SkipMessage {
  type: "skip";
  value: { generation: number; index: number };
}

interface RetrainMessage {
  type: "retrain";
  value: null;
}

interface CandidateChanges {
  accounts?: string[] | null;
  narration?: string | null;
  payee?: string | null;
  links?: string[] | null;
  tags?: string[] | null;
}

interface ChangeCandidateMessage {
  type: "change_candidate";
  value: {
    generation: number;
    candidate_index: number;
    changes: CandidateChanges;
  };
}

interface SelectCandidateMessage {
  type: "select_candidate";
  value: { index: number; generation: number };
}

interface GetFileContentsMessage {
  type: "get_file_contents";
  value: string;
}

interface WatchFileMessage {
  type: "watch_file";
  value: string;
}

interface UnwatchFileMessage {
  type: "unwatch_file";
  value: string;
}

interface SetFileContentsMessage {
  type: "set_file_contents";
  value: { filename: string; contents: string };
}

type ServerAction =
  | SkipMessage
  | RetrainMessage
  | ChangeCandidateMessage
  | SelectCandidateMessage
  | GetFileContentsMessage
  | SetFileContentsMessage
  | WatchFileMessage
  | UnwatchFileMessage;

type ServerStateListener = (state: Partial<ServerState>) => void;

class WatchedFileData {
  contents?: string;
  callbacks = new Map<WatchedFileHandle, (handle: WatchedFileHandle) => void>();
  needsUpdate = true;
}

export interface WatchedFileHandle {
  filename: string;
  needsUpdate: boolean;
  contents: string | undefined;
  cancel(): void;
  refresh(): void;
}

// The XXX portion of the key is replaced by the server with the randomly
// generated secret key when serving the client code.
export const secretKey =
  "BEANCOUNT_IMPORT_SECRET_KEY_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

export type ServerDataType = "pending" | "uncleared" | "invalid" | "errors";

export function fetchServerData(
  dataType: ServerDataType,
  generation: number,
  startIndex: number,
  endIndex: number
) {
  return fetch(
    `/${secretKey}/${dataType}/${generation}/${startIndex}-${endIndex}`
  );
}

const serverDataRetryDelay = 1000;

export class ServerListCache<T> {
  private generation: number = -1;
  private length: number = 0;
  private items: T[] = [];
  private requestedStartIndex = 0;
  private requestedEndIndex = 0;
  private blockStatus: (boolean | undefined)[] = [];
  public emitter = new EventEmitter();

  public callbacks = new Set<
    (startIndex: number, endIndex: number, generation: number) => void
  >();

  constructor(private dataType: ServerDataType, private blockSize = 10) {}

  requestRange(
    generation: number,
    length: number,
    startIndex: number,
    endIndex: number
  ) {
    if (generation !== this.generation) {
      this.generation = generation;
      this.length = length;
      this.items.length = 0;
      this.blockStatus.length = 0;
    }
    this.requestedStartIndex = startIndex;
    this.requestedEndIndex = endIndex;
    if (startIndex === endIndex) {
      return;
    }
    const { blockSize, blockStatus } = this;
    const startBlock = Math.floor(startIndex / blockSize);
    const endBlock = Math.floor((endIndex - 1) / blockSize);
    for (let blockIndex = startBlock; blockIndex <= endBlock; ++blockIndex) {
      if (blockStatus[blockIndex] === undefined) {
        this.fetchBlock(blockIndex);
      }
    }
  }

  get(generation: number, index: number): T | undefined {
    if (generation !== this.generation) {
      return undefined;
    }
    return this.items[index];
  }

  private fetchBlock(blockIndex: number) {
    this.blockStatus[blockIndex] = false;
    const { blockSize, generation } = this;
    const startIndex = blockIndex * blockSize;
    const endIndex = Math.min(this.length, startIndex + blockSize);
    const stillNeededOnFailure = () => {
      if (this.generation !== generation) {
        return false;
      }
      const { requestedStartIndex, requestedEndIndex } = this;
      if (requestedEndIndex <= startIndex || requestedStartIndex >= endIndex) {
        this.blockStatus[blockIndex] = undefined;
        return false;
      }
      return true;
    };
    fetchServerData(this.dataType, generation, startIndex, endIndex)
      .then(response => {
        if (response.ok) {
          return response.json();
        } else {
          throw new Error(
            `Error fetching response: ${response.status} ${response.statusText}`
          );
        }
      })
      .then(
        value => {
          if (this.generation !== generation) {
            return;
          }
          const { items } = this;
          for (let index = startIndex; index < endIndex; ++index) {
            items[index] = value[index - startIndex];
          }
          this.blockStatus[blockIndex] = true;
          const { requestedStartIndex, requestedEndIndex } = this;
          if (
            requestedEndIndex <= startIndex ||
            requestedStartIndex >= endIndex
          ) {
            return;
          }
          this.emitter.emit("received", generation, startIndex, endIndex);
        },
        error => {
          if (!stillNeededOnFailure()) {
            return;
          }
          console.log(
            `Error retrieving ${
              this.dataType
            }, ${generation}, ${startIndex}, ${endIndex}: ${error}`
          );
          setTimeout(() => {
            if (!stillNeededOnFailure()) {
              return;
            }
            this.fetchBlock(blockIndex);
          }, serverDataRetryDelay);
        }
      );
  }
}

export class ServerConnection {
  private ws: WebSocket;

  state: Partial<ServerState> = {};
  stateListeners = new Set<ServerStateListener>();

  addListener(listener: ServerStateListener, call = false) {
    this.stateListeners.add(listener);
    if (call) {
      listener(this.state);
    }
  }

  removeListener(listener: ServerStateListener) {
    this.stateListeners.delete(listener);
  }

  watchedFiles = new Map<string, WatchedFileData>();

  watchFile(
    filename: string,
    callback: (handle: WatchedFileHandle) => void
  ): WatchedFileHandle {
    let data = this.watchedFiles.get(filename);
    if (data === undefined) {
      data = new WatchedFileData();
      this.watchedFiles.set(filename, data);
      this.send({ type: "watch_file", value: filename });
    }
    let handle: WatchedFileHandle;
    handle = {
      get contents() {
        return data!.contents;
      },
      filename,
      get needsUpdate() {
        return data!.needsUpdate;
      },
      refresh: () => {
        data!.needsUpdate = true;
        this.send({ type: "get_file_contents", value: filename });
      },
      cancel: () => {
        data!.callbacks.delete(handle);
        if (data!.callbacks.size === 0) {
          this.send({ type: "unwatch_file", value: filename });
        }
      }
    };
    data.callbacks.set(handle, callback);
    if (data.contents !== undefined) {
      callback(handle);
    }
    return handle;
  }

  constructor() {
    const ws = (this.ws = new WebSocket(
      (window.location.protocol == "https:" ? "wss:" : "ws:") + "//" + window.location.host + window.location.pathname + secretKey + "/websocket"
    ));
    this.state["message"] = "Connecting to server";
    ws.onopen = () => {
      this.setState({ opened: true });
    };
    ws.onmessage = evt => {
      const data = JSON.parse(evt.data);
      if (data["type"] === "state_update") {
        this.setState(data["state"]);
      } else if (data["type"] === "file_contents") {
        const watchData = this.watchedFiles.get(data["path"]);
        if (watchData !== undefined) {
          watchData.contents = data["contents"];
          watchData.needsUpdate = false;
          for (const [handle, callback] of watchData.callbacks.entries()) {
            callback(handle);
          }
        }
      }
    };
    ws.onclose = () => {
      this.setState({ closed: true });
      this.tryToReload();
    };
  }

  private tryToReload() {
    let lastAttempt: number;
    const retryInterval = 2000;

    const tryToLoad = () => {
      lastAttempt = Date.now();
      fetch(window.location.href).then(
        () => {
          window.location.reload();
        },
        error => {
          const nextRequestTime = lastAttempt + retryInterval;
          const currentTime = Date.now();
          const delay = Math.max(0, nextRequestTime - Date.now());
          setTimeout(tryToLoad, delay);
        }
      );
    };
    tryToLoad();
  }

  private setState(state: any) {
    for (let key of Object.keys(state)) {
      let value = state[key];
      this.state[key as keyof ServerState] = value;
    }
    for (const listener of this.stateListeners) {
      listener(this.state);
    }
  }

  send(message: ServerAction) {
    this.ws.send(JSON.stringify(message));
  }

  skipBy(amount: number) {
    const { pending_index, pending } = this.state;
    if (pending_index == null || pending == null) {
      return;
    }
    const newIndex = pending_index + amount;
    if (newIndex < 0 || newIndex >= pending[1]) {
      return;
    }
    return this.skipTo(pending_index + amount);
  }

  skipTo(index: number) {
    const { pending } = this.state;
    if (pending == null) {
      return;
    }
    if (index < 0) {
      index += pending[1];
    }
    return executeServerCommand("skip", { generation: pending[0], index });
  }
}

export function executeServerCommand(command: string, msg: any): Promise<any> {
  return fetch(`/${secretKey}/${command}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json; charset=utf-8"
    },
    body: JSON.stringify(msg)
  }).then(response => response.json());
}

export function getServerFileUrl(path: string, contentType: string): string {
  return `/${secretKey}/get_file?path=${encodeURIComponent(
    path
  )}&content_type=${encodeURIComponent(contentType)}`;
}
