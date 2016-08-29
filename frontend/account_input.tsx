import * as React from "react";
import styled from "styled-components";
import Autocomplete from "react-autocomplete";
import commonPrefix from "common-prefix";
import { InputHTMLAttributes } from "react";

interface AccountInputComponentProps {
  accounts: string[];
  initial: string;
  onDone: (value?: string) => void;
}

interface AccountInputComponentState {
  confirming: boolean;
  value: string;
  completions: string[];
  prefix: string;
  hint: string;
}

const InputWrapperElement = styled.div`
  display: flex;
  flex: 1;
  position: relative;
  outline: 0px;
  border: 1px solid blue;
  margin: 0px;
  padding: 0px;
  left: 0;
  right: 0;
  bottom: 0;
  top: 0;
`;

const InputElement = styled.input.attrs({
  type: "text",
  spellCheck: false
})`
  position: relative;
  top: 0;
  left: 0;
  outline: 0;
  width: 100%;
  border: 0;
  margin: 0;
  padding: 0;
  background-color: transparent;
  box-shadow: none;
  font-family: monospace;
  font-size: medium;
`;

const InputHintElement = InputElement.extend.attrs({ disabled: true })`
  position: absolute;
  color: #aaa;
  height: 100%;
`;

const CompletionItem = styled<{ highlighted: boolean }, "div">("div")`
  font-family: monospace;
  background: ${p => (p.highlighted ? "lightblue" : "white")};
  cursor: default;
`;

const CompletionMenu = styled.div`
  margin-bottom: -1px;
  position: absolute;
  bottom: 100%;
  max-height: 50vh;
  overflow: auto;
  padding: 0;
  background-color: white;
  border: 1px solid black;
`;

export class AccountInputComponent extends React.PureComponent<
  AccountInputComponentProps,
  AccountInputComponentState
> {
  state = { ...this.getStateUpdateForValue({}, this.props.initial)! };

  hintRef = React.createRef<HTMLInputElement>();

  private getStateUpdateForValue(
    state: Partial<AccountInputComponentState>,
    value: string
  ) {
    if (value === state.value) {
      return null;
    }
    const completions = this.getCompletions(value);
    const prefix = commonPrefix(completions);
    const hint = prefix.startsWith(value) ? prefix : "";
    return { value, completions, prefix, hint, confirming: false };
  }

  private ref = React.createRef<Autocomplete>();
  render() {
    return (
      <label
        style={{ display: "flex", flexDirection: "row" }}
        onKeyDownCapture={this.handleKeyDown}
        onBlur={this.handleInputBlur}
      >
        {this.state.confirming ? "Confirm new account: " : "Account: "}
        <Autocomplete
          wrapperStyle={{
            display: "inline-flex",
            flex: 1,
            flexDirection: "row",
            position: "relative",
            marginLeft: "4px"
          }}
          ref={this.ref}
          open={this.state.completions.length > 0}
          items={this.state.completions}
          value={this.state.value}
          onSelect={this.handleSelect}
          onChange={this.handleChange}
          autoHighlight={false}
          getItemValue={this.getItemValue}
          renderItem={this.renderItem}
          renderMenu={this.renderMenu}
          renderInput={this.renderInput}
        />
      </label>
    );
  }

  private handleInputBlur = () => {
    const inputElement = this.ref.current!;
    // setTimeout is needed for Firefox.  Otherwise, calling focus() has no effect.
    setTimeout(() => inputElement.focus(), 0);
  };

  private renderInput = (props: any) => {
    const { ref, ...otherProps } = props;
    return (
      <InputWrapperElement>
        <InputHintElement value={this.state.hint} />
        <InputElement innerRef={ref} {...otherProps} />
      </InputWrapperElement>
    );
  };

  private getCompletions(value: string) {
    value = value.toLowerCase();
    let pattern = value.replace(/[.\[\]()?\\+\-{}]/g, "\\$&");
    pattern = pattern.replace(/:/g, ".*:");
    pattern = "(?:^|.*:)" + pattern;
    let regexp = new RegExp(pattern);
    let { accounts } = this.props;
    accounts.sort();
    const results = accounts.filter(account =>
      regexp.test(account.toLowerCase())
    );
    results.sort();
    return results;
  }

  componentDidMount() {
    const autoComplete = this.ref.current!;
    autoComplete.focus();
    autoComplete.select();
  }
  private renderMenu = (items: any[], value: string, styles: any) => {
    return (
      <CompletionMenu
        style={{
          position: "absolute",
          bottom: "100%",
          maxHeight: "50vh",
          overflow: "auto"
        }}
        children={items}
      />
    );
  };
  private renderItem = (item: string, isHighlighted: boolean, styles: any) => {
    return (
      <CompletionItem key={item} highlighted={isHighlighted}>
        {item}
      </CompletionItem>
    );
  };
  private getItemValue = (x: string) => x;
  private handleChange = (
    event: React.ChangeEvent<HTMLElement>,
    value: string
  ) => {
    this.setState(state => this.getStateUpdateForValue(state, value));
    const autoComplete = this.ref.current;
    if (autoComplete != null) {
      autoComplete.setState({ highlightedIndex: null });
    }
  };
  private handleSelect = (value: string) => {
    this.setState(state => this.getStateUpdateForValue(state, value));
  };
  private handleKeyDown = (event: React.KeyboardEvent<HTMLElement>) => {
    switch (event.key) {
      case "Escape": {
        this.props.onDone(undefined);
        break;
      }
      case "Enter": {
        const autoComplete = this.ref.current!;
        const { highlightedIndex } = autoComplete.state;
        if (
          highlightedIndex === null ||
          autoComplete.props.items[highlightedIndex] ===
            autoComplete.props.value
        ) {
          const value = this.state.value;
          if (this.state.completions.length === 0 && !this.state.confirming) {
            this.setState({ confirming: true });
            return;
          }
          this.props.onDone(value);
        }
        return;
      }
      case "Tab": {
        const autoComplete = this.ref.current!;
        const { highlightedIndex } = autoComplete.state;
        const items = autoComplete.props.items as string[];
        if (highlightedIndex !== null) {
          this.setState(state =>
            this.getStateUpdateForValue(state, items[highlightedIndex])
          );
          break;
        }
        if (items.length === 1) {
          autoComplete.setState({ highlightedIndex: 0 });
          this.setState(state => this.getStateUpdateForValue(state, items[0]));
          break;
        }
        const hint = this.state.hint;
        if (hint.length > 0) {
          this.setState(state => this.getStateUpdateForValue(state, hint));
          break;
        }
        return;
      }
      default:
        return;
    }
    event.preventDefault();
    event.stopPropagation();
  };
}
