import * as React from "react";
import * as ReactDOM from "react-dom";
import styled from "styled-components";
import { EventEmitter, EventSubscription } from "fbemitter";
import scrollIntoView from "scroll-into-view-if-needed";
import {
  Candidates,
  ServerConnection,
  BeancountTransaction,
  TransactionProperties,
  executeServerCommand,
  BeancountEntry
} from "./server_connection";
import { AccountInputComponent } from "./account_input";
import { UsedTransactionsComponent } from "./used_transactions";
import { CandidateComponent } from "./candidate";
import { AssociatedDataViewController } from "./app";
import { TransactionEditAction } from "./transaction_line_editor";

const CandidateListElement = styled.div`
  flex: 1;
  overflow-y: scroll;
  flex-basis: 0;
`;

export class CandidateSelectionState {
  selectedCandidateIndex: number = 0;
  candidates?: Candidates;
  selectedAssociatedDataIndex: number = 0;
  emitter = new EventEmitter();
  get selectedCandidate() {
    const { selectedCandidateIndex, candidates } = this;
    if (
      candidates == null ||
      selectedCandidateIndex < 0 ||
      selectedCandidateIndex >= candidates.candidates.length
    ) {
      return undefined;
    }
    return candidates.candidates[selectedCandidateIndex];
  }

  get selectedAssociatedData() {
    const { selectedCandidate } = this;
    if (selectedCandidate == null) {
      return undefined;
    }
    const { selectedAssociatedDataIndex } = this;
    const associatedData = selectedCandidate.associated_data;
    return associatedData[selectedAssociatedDataIndex];
  }

  setSelectedCandidate(
    candidates: Candidates | undefined,
    selectedCandidateIndex: number
  ) {
    if (
      candidates !== this.candidates ||
      selectedCandidateIndex !== this.selectedCandidateIndex
    ) {
      this.candidates = candidates;
      this.selectedCandidateIndex = selectedCandidateIndex;
      this.selectedAssociatedDataIndex = 0;
      this.emitter.emit("change");
    }
  }
  setSelectedAssociatedDataItem(index: number) {
    if (this.selectedAssociatedDataIndex !== index) {
      this.selectedAssociatedDataIndex = index;
      this.emitter.emit("change");
    }
  }
}

interface CandidatesComponentProps {
  candidates: Candidates;
  candidatesGeneration: number;
  serverConnection: ServerConnection;
  highlightPending: (index: number) => void;
  pendingIndex: number;
  numPending: number;
  accounts: string[];
  associatedDataViewController: AssociatedDataViewController;
  candidateSelectionState: CandidateSelectionState;
}

export type ActiveInputState = {
  type: "account";
  candidateIndex: number;
  groupNumber?: number;
  fieldNumber?: number;
  initial: string;
};

interface CandidatesComponentState {
  disabledUsedTransactions: Set<number>;
  hoverCandidateIndex?: number;
  candidates?: Candidates;
  candidatesGeneration?: number;
  inputState?: ActiveInputState;
  selectedCandidateIndex: number;
}

export class CandidatesComponent extends React.PureComponent<
  CandidatesComponentProps,
  CandidatesComponentState
> {
  state: CandidatesComponentState = {
    disabledUsedTransactions: new Set(),
    selectedCandidateIndex: 0
  };
  private filteredCandidateIndices: number[] = [];
  private globalToFilteredIndex = new Map<number, number>();
  private candidateRefs: (CandidateComponent | null)[] = [];

  static getDerivedStateFromProps(
    props: CandidatesComponentProps,
    state: CandidatesComponentState
  ) {
    const updates: Partial<CandidatesComponentState> = {};
    let hasUpdate = false;
    const candidates = props.candidates;
    let selectedCandidateIndex = state.selectedCandidateIndex;
    if (props.candidatesGeneration !== state.candidatesGeneration) {
      Object.assign(updates, {
        candidatesGeneration: props.candidatesGeneration,
        selectedCandidateIndex: 0
      });
      selectedCandidateIndex = 0;
      hasUpdate = true;
    }
    if (props.candidates !== state.candidates) {
      Object.assign(updates, {
        candidates: props.candidates,
        inputState: undefined
      });
      hasUpdate = true;
    }
    props.candidateSelectionState.setSelectedCandidate(
      candidates,
      selectedCandidateIndex
    );
    return hasUpdate ? updates : null;
  }

  handleUsedTransactionsChange = (index: number, value: boolean) => {
    const newDisabled = new Set(this.state.disabledUsedTransactions);
    if (value) {
      newDisabled.delete(index);
    } else {
      newDisabled.add(index);
    }
    this.setState({ disabledUsedTransactions: newDisabled });
  };

  get selectedCandidate() {
    const { candidates } = this.props.candidates;
    return candidates[this.state.selectedCandidateIndex];
  }

  get hoverCandidate() {
    const { hoverCandidateIndex } = this.state;
    if (hoverCandidateIndex === undefined) {
      return undefined;
    }
    const { candidates } = this.props.candidates;
    return candidates[hoverCandidateIndex];
  }

  private requestChangeAccount = (
    candidateIndex: number,
    spec: { groupNumber?: number; fieldNumber?: number }
  ) => {
    if (this.state.inputState !== undefined) {
      return;
    }
    const candidate = this.props.candidates.candidates[candidateIndex];
    const substituted = candidate.substituted_accounts;
    const fieldNumber = substituted.findIndex(
      ([uniqueName, accountName, groupNumber], fieldNumber) =>
        (spec.groupNumber === undefined || groupNumber === spec.groupNumber) &&
        (spec.fieldNumber === undefined || fieldNumber === spec.fieldNumber)
    );
    if (fieldNumber === -1) {
      return;
    }
    const [uniqueName, accountName, groupNumber] = substituted[fieldNumber];
    this.setState({
      inputState: {
        type: "account",
        candidateIndex,
        initial: accountName,
        fieldNumber: spec.fieldNumber,
        groupNumber: spec.groupNumber
      }
    });
  };

  changeAccount = (
    candidateIndex: number,
    newValue: string,
    spec: { groupNumber?: number; fieldNumber?: number }
  ) => {
    const candidate = this.props.candidates.candidates[candidateIndex];
    const substituted = candidate.substituted_accounts;
    let newAccounts: string[];
    if (spec.groupNumber !== undefined) {
      newAccounts = substituted.map(
        ([uniqueName, accountName, groupNumber]) => {
          if (groupNumber === spec.groupNumber) return newValue;
          return accountName;
        }
      );
    } else if (spec.fieldNumber !== undefined) {
      newAccounts = substituted.map(
        ([uniqueName, accountName, groupNumber], fieldNumber) => {
          if (fieldNumber === spec.fieldNumber) return newValue;
          return accountName;
        }
      );
    } else {
      newAccounts = substituted.map(() => newValue);
    }
    this.sendChangeAccounts(candidateIndex, newAccounts);
  };

  private sendChangeAccounts(candidateIndex: number, newAccounts: string[]) {
    const candidate = this.props.candidates.candidates[candidateIndex];
    const transaction = candidate.new_entries[0] as BeancountTransaction;
    return executeServerCommand("change_candidate", {
      generation: this.props.candidatesGeneration,
      candidate_index: candidateIndex,
      changes: {
        accounts: newAccounts,
        tags: transaction.tags,
        links: transaction.links,
        narration: transaction.narration,
        payee: transaction.payee
      }
    });
  }

  private skipToNext = () => {
    this.props.serverConnection.skipBy(1);
  };

  private skipToFirst = () => {
    this.props.serverConnection.skipTo(0);
  };

  private skipToLast = () => {
    this.props.serverConnection.skipTo(-1);
  };

  private skipToPrior = () => {
    this.props.serverConnection.skipBy(-1);
  };

  private retrain = () => {
    executeServerCommand("retrain", null);
  };

  private changeSelectedCandidateAllAccounts = () => {
    this.requestChangeAccount(this.state.selectedCandidateIndex, {});
  };

  private fixme = () => {
    const { inputState } = this.state;
    if (inputState !== undefined) {
      return;
    }
    const candidate = this.selectedCandidate;
    const substituted = candidate.substituted_accounts;
    if (substituted.length === 0) return Promise.resolve(undefined);
    const newAccounts = substituted.map(x => x[3]);
    return this.sendChangeAccounts(
      this.state.selectedCandidateIndex,
      newAccounts
    );
  };

  private handleAccountInput = (value?: string) => {
    const { inputState } = this.state;
    if (inputState !== undefined) {
      if (value !== undefined && inputState.type === "account") {
        this.changeAccount(inputState.candidateIndex, value, inputState);
      }
    }
    this.setState({ inputState: undefined });
  };

  render() {
    const selectedCandidate = this.selectedCandidate;
    const hoverCandidate = this.hoverCandidate;
    const { disabledUsedTransactions, inputState } = this.state;
    const selectedUsedTransactions =
      selectedCandidate === undefined
        ? []
        : selectedCandidate.used_transaction_ids;
    const hoverUsedTransactions =
      hoverCandidate === undefined ? [] : hoverCandidate.used_transaction_ids;
    const { filteredCandidateIndices, globalToFilteredIndex } = this;
    this.filteredCandidateIndices.length = 0;
    this.globalToFilteredIndex.clear();
    const hasAccountSubstitutions =
      selectedCandidate !== undefined &&
      selectedCandidate.substituted_accounts.length > 0;
    const { numPending, pendingIndex } = this.props;
    let accountInputComponent: any;

    if (inputState !== undefined) {
      const accountSet = new Set(this.props.accounts);
      const candidate = this.props.candidates.candidates[
        inputState.candidateIndex
      ];
      if (candidate !== undefined) {
        const substitutions = candidate.substituted_accounts;
        for (const [
          uniqueName,
          accountName,
          groupNumber,
          originalName
        ] of substitutions) {
          accountSet.add(accountName);
          accountSet.add(originalName);
        }
        for (const entry of candidate.new_entries) {
          if (entry.hasOwnProperty("postings")) {
            for (const posting of (entry as BeancountTransaction).postings) {
              accountSet.add(posting.account);
            }
          }
        }
      }
      accountInputComponent = (
        <AccountInputComponent
          initial={inputState.initial}
          accounts={Array.from(accountSet)}
          onDone={this.handleAccountInput}
        />
      );
    }

    return (
      <>
        <div className="action-button-wrapper">
          <div className="action-button__group">
            <button
              disabled={pendingIndex == 0}
              onClick={this.skipToFirst}
              title="Skip to first pending entry"
              className="action-button"
            >
              First
            </button>
            <button
              disabled={pendingIndex == 0}
              onClick={this.skipToPrior}
              title="Skip to previous pending entry, keyboard shortcut: ["
              className="action-button"
            >
              Previous
            </button>
            <button
              disabled={pendingIndex + 1 >= numPending}
              onClick={this.skipToNext}
              title="Skip to next pending entry, keyboard shortcut: ]"
              className="action-button"
            >
              Next
            </button>
            <button
              disabled={pendingIndex + 1 >= numPending}
              onClick={this.skipToNext}
              title="Skip to last pending entry"
              className="action-button"
            >
              Last
            </button>
          </div>
          <div className="action-button__group">
            <button
              disabled={!hasAccountSubstitutions}
              onClick={this.changeSelectedCandidateAllAccounts}
              title="Change all unknown accounts to the same value, keyboard shortcut: a"
              className="action-button"
            >
              Account
            </button>
            <button
              disabled={!hasAccountSubstitutions}
              onClick={this.fixme}
              title="Reset all unknown accounts of the selected candidate to FIXME accounts, keyboard shortcut: f"
              className="action-button"
            >
              Fixme
            </button>
            <button
              disabled={
                selectedCandidate.original_transaction_properties == null
              }
              onClick={this.handleEditPayee}
              title="Edit payee of selected candidate, keyboard shortcut: p"
              className="action-button"
            >
              Payee
            </button>
            <button
              disabled={
                selectedCandidate.original_transaction_properties == null
              }
              onClick={this.handleEditNarration}
              title="Edit narration of selected candidate, keyboard shortcut: n"
              className="action-button"
            >
              Narration
            </button>
            <button
              disabled={
                selectedCandidate.original_transaction_properties == null
              }
              onClick={this.handleAddLink}
              title="Add link to selected candidate, keyboard shortcut: ^"
              className="action-button"
            >
              Link
            </button>
            <button
              disabled={
                selectedCandidate.original_transaction_properties == null
              }
              onClick={this.handleAddTag}
              title="Add link to selected candidate, keyboard shortcut: #"
              className="action-button"
            >
              Tag
            </button>
          </div>
          <div className="action-button__group">
          <button
              onClick={this.handleConfirm}
              title="Confirm selected candidate, keyboard shortcut: enter"
              className="action-button"
            >
              Confirm
            </button>
            <button
              disabled={
                selectedCandidate.original_transaction_properties == null
              }
              onClick={this.handleRevert}
              title="Revert changes to selected candidate"
              className="action-button"
            >
              Revert
            </button>
            <button
              onClick={this.handleIgnore}
              title="Add the selected candidate to the ignore file, keyboard shortcut: i"
              className="action-button"
            >
              Ignore
            </button>
          </div>
          <div className="action-button__group">
            <button
              onClick={this.retrain}
              title="Retrain classifier, keyboard shortcut: t"
              className="action-button"
            >
              Retrain
            </button>
          </div>
        </div>
        <UsedTransactionsComponent
          usedTransactions={this.props.candidates.used_transactions}
          disabledUsedTransactions={this.state.disabledUsedTransactions}
          selectedUsedTransactions={selectedUsedTransactions}
          hoverUsedTransactions={hoverUsedTransactions}
          onChange={this.handleUsedTransactionsChange}
        />
        <CandidateListElement>
          {this.props.candidates.candidates.map((candidate, index) => {
            for (const usedTransactionId of candidate.used_transaction_ids) {
              if (disabledUsedTransactions.has(usedTransactionId)) {
                return null;
              }
            }
            this.globalToFilteredIndex.set(
              index,
              this.filteredCandidateIndices.length
            );
            this.filteredCandidateIndices.push(index);
            return (
              <CandidateComponent
                ref={x => {
                  this.candidateRefs[index] = x;
                }}
                selected={candidate === selectedCandidate}
                hover={index === this.state.hoverCandidateIndex}
                onSelect={this.selectCandidate}
                onAccept={this.acceptCandidate}
                onHover={this.setHoverCandidate}
                inputState={
                  inputState !== undefined &&
                  inputState.candidateIndex === index
                    ? inputState
                    : undefined
                }
                candidate={candidate}
                candidateIndex={index}
                key={index}
                changeAccount={this.requestChangeAccount}
                changeTransactionProperties={
                  this.handleChangeTransactionProperties
                }
              />
            );
          })}
        </CandidateListElement>
        {accountInputComponent}
      </>
    );
  }

  private handleChangeTransactionProperties = (
    candidateIndex: number,
    properties: TransactionProperties
  ) => {
    const candidate = this.props.candidates.candidates[candidateIndex];
    const transaction = candidate.new_entries[0] as BeancountTransaction;
    const substituted = candidate.substituted_accounts;
    const newAccounts = substituted.map(
      ([uniqueName, accountName]) => accountName
    );
    executeServerCommand("change_candidate", {
      generation: this.props.candidatesGeneration,
      candidate_index: candidateIndex,
      changes: {
        accounts: newAccounts,
        tags: properties.tags,
        links: properties.links,
        narration: properties.narration,
        payee: properties.payee
      }
    });
  };

  componentDidMount() {
    window.addEventListener("keydown", this.handleKeyDown);
  }

  componentWillUnmount() {
    window.removeEventListener("keydown", this.handleKeyDown);
  }

  private handleKeyDown = (event: KeyboardEvent) => {
    if (
      event.target instanceof HTMLInputElement ||
      event.target instanceof HTMLTextAreaElement ||
      event.target instanceof HTMLButtonElement
    ) {
      return;
    }
    switch (event.key) {
      case "[":
        this.skipToPrior();
        break;
      case "]":
        this.skipToNext();
        break;
      case "a":
        this.changeSelectedCandidateAllAccounts();
        break;
      case "f":
        this.fixme();
        break;
      case "t":
        this.retrain();
        break;
      case "1":
        this.requestChangeAccount(this.state.selectedCandidateIndex, {
          groupNumber: 0
        });
        break;
      case "2":
        this.requestChangeAccount(this.state.selectedCandidateIndex, {
          groupNumber: 1
        });
        break;
      case "3":
        this.requestChangeAccount(this.state.selectedCandidateIndex, {
          groupNumber: 2
        });
        break;
      case "4":
        this.requestChangeAccount(this.state.selectedCandidateIndex, {
          groupNumber: 3
        });
        break;
      case "5":
        this.requestChangeAccount(this.state.selectedCandidateIndex, {
          groupNumber: 4
        });
        break;
      case "6":
        this.requestChangeAccount(this.state.selectedCandidateIndex, {
          groupNumber: 5
        });
        break;
      case "7":
        this.requestChangeAccount(this.state.selectedCandidateIndex, {
          groupNumber: 6
        });
        break;
      case "8":
        this.requestChangeAccount(this.state.selectedCandidateIndex, {
          groupNumber: 7
        });
        break;
      case "9":
        this.requestChangeAccount(this.state.selectedCandidateIndex, {
          groupNumber: 8
        });
        break;
      case "0":
        this.requestChangeAccount(this.state.selectedCandidateIndex, {
          groupNumber: 9
        });
        break;
      case "ArrowUp":
        this.selectCandidateRelative(-1);
        break;
      case "ArrowDown":
        this.selectCandidateRelative(1);
        break;
      case "Enter":
        this.acceptCandidate(this.state.selectedCandidateIndex, {
          showInEditor: event.shiftKey
        });
        break;
      case "i":
        this.ignoreCandidate(event.shiftKey);
        break;
      case "#":
        this.editCurrentTransaction("tag");
        break;
      case "^":
        this.editCurrentTransaction("link");
        break;
      case "n":
        this.editCurrentTransaction("narration");
        break;
      case "p":
        this.editCurrentTransaction("payee");
        break;
      default:
        return;
    }
    event.stopPropagation();
    event.preventDefault();
  };

  private editCurrentTransaction(action: TransactionEditAction) {
    const candidateIndex = this.state.selectedCandidateIndex;
    const candidateRef = this.candidateRefs[candidateIndex];
    if (candidateRef == null) {
      return;
    }
    candidateRef.startEdit(action);
  }

  private acceptCandidate = (
    candidateIndex: number,
    { showInEditor = false, ignore = false } = {}
  ) => {
    const promise = executeServerCommand("select_candidate", {
      index: candidateIndex,
      generation: this.props.candidatesGeneration,
      ignore
    });
    promise.then((newEntries: BeancountEntry[]) => {
      if (newEntries.length > 0) {
        this.props.associatedDataViewController.selectFileByMeta(
          newEntries[0]["meta"],
          {
            focus: showInEditor,
            refresh: true,
            switchTo: showInEditor
          }
        );
      }
    });
    return promise;
  };

  private selectCandidateRelative(amount: number) {
    this.setState(state => {
      const currentGlobalIndex = state.selectedCandidateIndex;
      const currentFilteredIndex = this.globalToFilteredIndex.get(
        currentGlobalIndex
      )!;
      const { filteredCandidateIndices } = this;
      const newFilteredIndex =
        (currentFilteredIndex + amount + filteredCandidateIndices.length) %
        filteredCandidateIndices.length;
      const newGlobalIndex = filteredCandidateIndices[newFilteredIndex];
      const candidateComponent = this.candidateRefs[newGlobalIndex];
      if (candidateComponent != null) {
        const candidateElement = ReactDOM.findDOMNode(
          candidateComponent
        ) as Element | null;
        if (candidateElement != null) {
          scrollIntoView(candidateElement);
        }
      }
      return {
        selectedCandidateIndex: newGlobalIndex
      };
    });
  }

  private selectCandidate = (candidateIndex: number) => {
    this.setState({ selectedCandidateIndex: candidateIndex });
  };

  private setHoverCandidate = (candidateIndex: number, value: boolean) => {
    this.setState({ hoverCandidateIndex: value ? candidateIndex : undefined });
  };

  private handleRevert = () => {
    const candidateIndex = this.state.selectedCandidateIndex;
    const candidate = this.props.candidates.candidates[candidateIndex];
    const transaction = candidate.new_entries[0] as BeancountTransaction;
    const substituted = candidate.substituted_accounts;
    const newAccounts = substituted.map(
      ([uniqueName, accountName]) => accountName
    );
    return executeServerCommand("change_candidate", {
      generation: this.props.candidatesGeneration,
      candidate_index: candidateIndex,
      changes: {}
    });
  };

  private ignoreCandidate(showInEditor = false) {
    const candidateIndex = this.state.selectedCandidateIndex;
    this.fixme()!.then(() =>
      this.acceptCandidate(candidateIndex, { showInEditor, ignore: true })
    );
  }

  private handleIgnore = () => {
    this.ignoreCandidate(false);
  };

  private handleAddLink = () => {
    this.editCurrentTransaction("link");
  };
  private handleAddTag = () => {
    this.editCurrentTransaction("tag");
  };
  private handleEditNarration = () => {
    this.editCurrentTransaction("narration");
  };
  private handleEditPayee = () => {
    this.editCurrentTransaction("payee");
  };
  private handleConfirm = () => {
    this.acceptCandidate(this.state.selectedCandidateIndex);
  };
}
