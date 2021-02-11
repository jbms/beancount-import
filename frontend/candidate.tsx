import * as React from "react";
import styled from "styled-components";
import {
  Candidate,
  LineChangeType,
  TransactionProperties
} from "./server_connection";
import { ActiveInputState } from "./candidates";
import {
  TransactionLineEditorComponent,
  TransactionEditAction
} from "./transaction_line_editor";

interface CandidateComponentProps {
  candidate: Candidate;
  candidateIndex: number;
  changeAccount: (
    candidateIndex: number,
    target: { groupNumber: number } | { fieldNumber: number }
  ) => void;
  changeTransactionProperties: (
    candidateIndex: number,
    properties: TransactionProperties
  ) => void;
  selected: boolean;
  hover: boolean;
  onSelect: (candidateIndex: number) => void;
  onAccept: (candidateIndex: number) => void;
  onHover: (candidateIndex: number, value: boolean) => void;
  inputState?: ActiveInputState;
}

const groupBackgroundColors = [
  "var(--color-bg-group-1)",
  "var(--color-bg-group-2)",
  "var(--color-bg-group-3)",
  "var(--color-bg-group-4)",
  "var(--color-bg-group-5)",
  "var(--color-bg-group-6)",
  "var(--color-bg-group-7)"
];

const AccountSubstitutionBackground = styled<{ groupNumber: number }, "div">(
  "div"
)`
  background-color: ${p =>
    groupBackgroundColors[p.groupNumber % groupBackgroundColors.length]};
`;

const AccountSubstitutionElement = styled<{ active: boolean }, "div">("div")`
  display: inline;
  position: relative;
  cursor: pointer;
  border: 1px solid ${p => (p.active ? "black" : "transparent")};

  :hover {
    text-decoration: underline;
  }
`;

const AccountSubstitutionIndicatorElement = styled.div`
  display: inline-block;
  position: absolute;
  top: -3px;
  color: black;
  font-weight: bold;
  right: 100%;
  padding: 1px;
  border: 2px solid #000;
`;

const lineChangeElements = new Map([
  [
    LineChangeType.delete,
    styled.div`
      color: var(--color-line-change-delete);
    `
  ],
  [LineChangeType.context, styled.div``],
  [
    LineChangeType.insert,
    styled.div`
      color: var(--color-line-change-add);
    `
  ]
]);

const lineChangePrefix: { [index: string]: string } = {
  "-1": "-",
  "0": " ",
  "1": "+"
};

const CandidateChangesElement = styled<
  { selected: boolean; hover: boolean },
  "div"
>("div")`
  font-family: monospace;
  white-space: pre;
  border: 1px solid ${props => (props.hover ? "black" : "transparent")};
  opacity: ${p => (p.selected ? 1.0 : 0.75)};
  position: relative;
  padding: 2px;
  margin-top: 1em;
  margin-bottom: 1em;
  margin-left: 7px;

  :before {
    content: " ";
    position: absolute;
    left: -5px;
    right: 0px;
    bottom: 0px;
    top: 0px;
    z-index: -1;
    border-left: 4px solid ${p => (p.selected ? "blue" : "transparent")};
  }
`;

export class CandidateComponent extends React.PureComponent<
  CandidateComponentProps
> {
  private transactionLineEditor = React.createRef<
    TransactionLineEditorComponent
  >();
  private *getLineChanges() {
    for (const [, changeSets] of this.props.candidate.change_sets) {
      for (let [, lineChanges] of changeSets) {
        yield* lineChanges;
      }
    }
  }

  private handleAccountClick = (event: React.MouseEvent<HTMLDivElement>) => {
    const { currentTarget } = event;
    this.props.changeAccount(
      this.props.candidateIndex,
      event.shiftKey
        ? { fieldNumber: parseInt(currentTarget.dataset.fieldNumber!) }
        : { groupNumber: parseInt(currentTarget.dataset.groupNumber!) }
    );
  };

  private handleSelect = () => {
    this.props.onSelect(this.props.candidateIndex);
  };

  private handleMouseEnter = () => {
    this.props.onHover(this.props.candidateIndex, true);
  };

  private handleMouseLeave = () => {
    this.props.onHover(this.props.candidateIndex, false);
  };

  private handleTransactionLineChange = (properties: TransactionProperties) => {
    this.props.changeTransactionProperties(
      this.props.candidateIndex,
      properties
    );
  };

  render() {
    const { candidate } = this.props;
    const substituted = candidate.substituted_accounts;
    const { inputState } = this.props;
    let waitingForTransactionStart =
      candidate.original_transaction_properties != null;
    let currentChangeType: LineChangeType | undefined;
    let currentElements: any[] = [];
    let output: any[] = [];
    let lastElementIsString = false;
    const flushCurrent = () => {
      if (currentElements.length > 0) {
        const LineChangeElement = lineChangeElements.get(currentChangeType!)!;
        output.push(
          <LineChangeElement key={output.length}>
            {currentElements}
          </LineChangeElement>
        );
        currentElements = [];
        lastElementIsString = false;
      }
    };
    const setChangeType = (changeType: LineChangeType) => {
      if (changeType !== currentChangeType) {
        flushCurrent();
        currentChangeType = changeType;
      }
    };
    const addText = (s: string) => {
      if (!s) return;
      if (lastElementIsString) {
        currentElements[currentElements.length - 1] += s;
      } else {
        lastElementIsString = true;
        currentElements.push(s);
      }
    };
    const addElement = (x: any) => {
      currentElements.push(x);
      lastElementIsString = false;
    };

    for (const [changeType, line] of this.getLineChanges()) {
      setChangeType(changeType);
      addText(lineChangePrefix[changeType]);
      if (
        waitingForTransactionStart &&
        changeType !== LineChangeType.delete &&
        line.length > 0
      ) {
        const quoteStart = line.indexOf('"');
        addText(line.substring(0, quoteStart));
        addElement(
          <TransactionLineEditorComponent
            ref={this.transactionLineEditor}
            key={currentElements.length}
            candidate={candidate}
            changeType={changeType}
            value={line.substring(quoteStart)}
            onChange={this.handleTransactionLineChange}
          />
        );
        addText("\n");
        waitingForTransactionStart = false;
      } else {
        let start = 0;
        let fieldNumber = 0;
        for (const [uniqueName, accountName, groupNumber] of substituted) {
          const i = line.indexOf(uniqueName);
          if (i === -1) {
            ++fieldNumber;
            continue;
          }
          const active =
            inputState !== undefined &&
            (inputState.fieldNumber === undefined ||
              inputState.fieldNumber === fieldNumber) &&
            (inputState.groupNumber === undefined ||
              inputState.groupNumber == groupNumber);
          addText(line.substring(0, i));
          const groupBackgroundColor =
            groupBackgroundColors[groupNumber % groupBackgroundColors.length];
          addElement(
            <AccountSubstitutionElement
              title={`Click to change all group ${groupNumber +
                1} posting accounts (keyboard shortcut ${groupNumber +
                1}).   Shift click to change this posting account only.`}
              active={active}
              key={currentElements.length}
              style={{ backgroundColor: groupBackgroundColor }}
              data-field-number={fieldNumber}
              data-group-number={groupNumber}
              onClick={this.handleAccountClick}
            >
              <AccountSubstitutionIndicatorElement
                style={{ backgroundColor: groupBackgroundColor }}
              >
                {"" + (groupNumber + 1)}
              </AccountSubstitutionIndicatorElement>
              {accountName}
            </AccountSubstitutionElement>
          );
          start = i + uniqueName.length;
          break;
        }
        addText(line.substring(start));
        addText("\n");
      }
    }
    flushCurrent();
    return (
      <CandidateChangesElement
        selected={this.props.selected}
        hover={this.props.hover}
        onClick={this.handleSelect}
        onMouseEnter={this.handleMouseEnter}
        onMouseLeave={this.handleMouseLeave}
        onDoubleClick={this.handleAccept}
      >
        {output}
      </CandidateChangesElement>
    );
  }

  private handleAccept = () => {
    this.props.onAccept(this.props.candidateIndex);
  };

  startEdit(action: TransactionEditAction) {
    const field = this.transactionLineEditor.current;
    if (field !== null) {
      field.startEdit(action);
    }
  }
}
