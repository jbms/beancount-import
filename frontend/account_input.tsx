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

const InputLabelElement = styled.label`
  display: flex;
  flex-direction: row;
  padding: 4px;
  line-height: 22px;
  vertical-align: middle;
  background-color: var(--color-main-bg);
  box-sizing: border-box;
  font-size: var(--font-size-sans-reg);
  border-top: 1px solid var(--color-main-accent);
`;

const InputWrapperElement = styled.div`
  display: flex;
  flex: 1;
  position: relative;
  outline: 0px;
  border: 1px solid var(--color-main-accent);
  margin: 0px;
  padding: 2px 8px;
`;

const InputElement = styled.input.attrs({
  type: "text",
  spellCheck: false
})`
  position: relative;
  outline: 0;
  width: 100%;
  border: 0;
  margin: 0;
  padding: 0;
  /* transparent so the HintElement can "shine through" for autocompletion */
  background-color: transparent;
  color: var(--color-main-text);
  box-shadow: none;
  font-family: var(--font-fam-mono);
  font-size: var(--font-size-mono-reg);
`;

const InputHintElement = InputElement.extend.attrs({ disabled: true })`
  position: absolute;
  height: 100%;
  top: 0px;
  left: 8px;
  color: var(--color-main-accent);
`;

const CompletionItem = styled<{ highlighted: boolean }, "div">("div")`
  font-family: var(--font-fam-mono);
  font-size: var(--font-size-mono-reg);
  cursor: pointer;
  line-height: 1;
  padding: 2px 8px;
  ${props =>
    props.highlighted &&
    `
    background-color: var(--color-select-bg);
    color: var(--color-select-text);
    
    `};
`;

const CompletionMenu = styled.div`
  margin-bottom: -1px;
  margin-left: -0px;
  position: absolute;
  bottom: 100%;
  max-height: 50vh;
  overflow: auto;
  padding: 0px;
  background-color: var(--color-main-bg);
  border: 1px solid var(--color-main-accent);
  border-top-left-radius: 5px;
  border-top-right-radius: 5px;
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
      <InputLabelElement
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
      </InputLabelElement>
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
    return <CompletionMenu children={items} />;
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
