import * as React from "react";
import * as ReactDOM from "react-dom";
import styled from "styled-components";
import {
  Candidate,
  LineChangeType,
  TransactionProperties
} from "./server_connection";

interface TransactionLineParseResult {
  properties: TransactionProperties;
  payeeStart: number;
  payeeEnd: number;
  narrationStart: number;
  narrationEnd: number;
  tagsAndLinksStart: number;
  tagsAndLinksEnd: number;
}

export type TransactionEditAction = "link" | "tag" | "narration" | "payee";

interface TransactionLineEditorProps {
  candidate: Candidate;
  changeType: LineChangeType;
  value: string;
  onChange: (properties: TransactionProperties) => void;
}

interface TransactionLineEditorState {
  candidate?: Candidate;
  value?: string;
  parseResult?: { value: string; result: TransactionLineParseResult | null };
}

const LineEditorElement = styled.input<{ valid: boolean }>`
  color: inherit;
  font-family: inherit;
  font-size: inherit;
  outline: 0px;
  border: 0px;
  padding: 0;
  margin: 0;
  background-color: transparent;
  
  :focus {
    background-color: var(--color-main-bg);
    color: var(--color-main-text);
    outline: 1px solid ${p => (p.valid ? "var(--color-main-accent)" : "var(--color-line-change-delete)")};
  }
`;

const linePattern = /^(\s*)(?:("(?:[^"\\]|\\.)*")(\s+))?("(?:[^"\\]|\\.)*")((?:\s+[#^][A-Za-z0-9\-_\/.]+)*)\s*$/;

function parseTransactionLine(line: string): TransactionLineParseResult | null {
  const m = line.match(linePattern);
  if (m === null) return null;
  try {
    let payee: string | null = null;
    let payeeStart = -1,
      payeeEnd = -1;
    let offset = m[1].length;
    if (m[2] !== undefined) {
      payee = JSON.parse(m[2]);
      payeeStart = offset;
      payeeEnd = payeeStart + m[2].length;
      offset = payeeEnd + m[3].length;
    }
    const narrationStart = offset;
    const narration = JSON.parse(m[4]);
    const narrationEnd = narrationStart + m[4].length;
    const tagsAndLinksStart = narrationEnd;
    const tagsAndLinksEnd = line.length;
    const tags: string[] = [];
    const links: string[] = [];
    const tagsAndLinks = m[5].trim();
    if (tagsAndLinks.length !== 0) {
      for (const x of tagsAndLinks.split(/\s+/)) {
        (x.startsWith("#") ? tags : links).push(x.substring(1));
      }
    }
    return {
      payeeStart,
      payeeEnd,
      narrationStart,
      narrationEnd,
      tagsAndLinksStart,
      tagsAndLinksEnd,
      properties: { payee, narration, tags, links }
    };
  } catch {
    return null;
  }
}

export class TransactionLineEditorComponent extends React.PureComponent<
  TransactionLineEditorProps,
  TransactionLineEditorState
> {
  state: TransactionLineEditorState = {};
  static getDerivedStateFromProps(
    props: TransactionLineEditorProps,
    state: TransactionLineEditorState
  ) {
    const update: Partial<TransactionLineEditorState> = {};
    let value = state.value;
    if (value === undefined || state.candidate != props.candidate) {
      value = props.value;
      update.value = value;
      update.candidate = props.candidate;
    }
    if (state.parseResult === undefined || state.parseResult.value !== value) {
      update.parseResult = { value, result: parseTransactionLine(value) };
    }
    return update;
  }
  render() {
    const { state } = this;
    const value = state.value!;
    return (
      <LineEditorElement
        type="text"
        spellCheck={false}
        autoComplete="off"
        valid={state.parseResult!.result !== null}
        onDoubleClick={this.handleDoubleClick}
        value={value}
        style={{ width: `${value.length + 1}ch` }}
        onBlur={this.handleBlur}
        onChange={this.handleChange}
        onKeyDown={this.handleKeyDown}
      />
    );
  }

  private handleDoubleClick = (event: React.MouseEvent) => {
    event.stopPropagation();
  };

  private handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    this.setState({ value: event.target.value });
  };

  private handleBlur = (event: React.FocusEvent<HTMLInputElement>) => {
    if (this.state.value === this.props.value) {
      return;
    }
    const parseResult = this.state.parseResult!;
    if (parseResult.result !== null) {
      this.props.onChange(parseResult.result.properties);
    } else {
      const element = ReactDOM.findDOMNode(this) as HTMLInputElement;
      // setTimeout is needed for Firefox.  Otherwise, calling focus() has no
      // effect.
      setTimeout(() => element.focus(), 0);
    }
  };

  private handleKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    const element = ReactDOM.findDOMNode(this) as HTMLInputElement;
    switch (event.key) {
      case "Escape": {
        this.setState({ value: this.props.value }, () => {
          element.blur();
        });
        break;
      }
      case "Enter": {
        element.blur();
        break;
      }
    }
  };

  private select(start: number, end: number) {
    const element = ReactDOM.findDOMNode(this) as HTMLInputElement | null;
    if (element === null) {
      return;
    }
    element.focus();
    element.setSelectionRange(start, end, "forward");
  }

  private addTagOrLink(startChar: string) {
    const { value } = this.state;
    if (value === undefined) {
      return;
    }
    const newValue = value.trim() + " " + startChar;
    console.log(newValue);
    this.setState({ value: newValue }, () =>
      this.select(newValue.length, newValue.length)
    );
  }

  startEdit(action: TransactionEditAction) {
    switch (action) {
      case "tag":
        return this.addTagOrLink("#");
      case "link":
        return this.addTagOrLink("^");
      case "narration":
        return this.editNarration();
      case "payee":
        return this.editPayee();
    }
  }

  editNarration() {
    const { parseResult } = this.state;
    if (parseResult === undefined || parseResult.result === null) {
      return;
    }
    this.select(
      parseResult.result.narrationStart + 1,
      parseResult.result.narrationEnd - 1
    );
  }

  editPayee() {
    const { parseResult } = this.state;
    if (parseResult === undefined || parseResult.result === null) {
      return;
    }
    this.select(
      parseResult.result.payeeStart + 1,
      parseResult.result.payeeEnd - 1
    );
  }
}
