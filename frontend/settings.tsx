import * as React from "react";
import styled from "styled-components";

const SettingsComponentRoot = styled.div`
  position: relative;
`;

const SettingsComponentInnerContainer = styled.div`
  width: 280px;
  box-sizing: border-box;
  position: absolute;
  bottom: 24px;
  right: 0;
  padding: 8px;
  background-color: var(--color-main-bg);
  border: 1px solid var(--color-main-accent);
  border-radius: 5px;
`;

const SettingsButton = styled.button`
  font-family: var(--font-fam-sans);
  font-size: var(--font-size-sans-reg);
  border: 0;
  background-color: transparent;
  color: var(--color-main-text);
  cursor: pointer;
`;

const Label = styled.label`
  font-weight: bold;
  display: block;
  margin-bottom: 4px;
`;

const ThemeSelectorElm = styled.select`
  margin-bottom: 12px;
  cursor: pointer;
  width: 100%;
`;

const CandidatesPositionElm = styled.select`
  margin-bottom: 12px;
  cursor: pointer;
  width: 100%;
`;

interface SettingsComponentProps {
  isOpen: boolean;
  onToggle: (open?: boolean) => void;
  onSettingsChange: (settings: SettingsComponentState) => void;
}

export interface SettingsComponentState {
  theme: string;
  candidatesLeft: boolean;
}

function applyTheme(theme: string) {
  // always remove all classes to handle "system default" case
  window.document.body.classList.remove("theme-dark");
  window.document.body.classList.remove("theme-light");

  if (theme === "dark") {
    window.document.body.classList.add("theme-dark");
  } else if (theme === "light") {
    window.document.body.classList.add("theme-light");
  }
}

export class SettingsComponent extends React.PureComponent<
  SettingsComponentProps,
  SettingsComponentState
> {
  state: SettingsComponentState = {
    theme: "auto",
    candidatesLeft: true
  };

  private handleThemeChange = ({
    target
  }: React.ChangeEvent<HTMLSelectElement>) => {
    const theme = target.value;
    this.setState({ theme });
    applyTheme(theme);
    window.localStorage.setItem("theme", theme);
  };

  private handleCandidatesPositionChange = ({
    target
  }: React.ChangeEvent<HTMLSelectElement>) => {
    const candidatesLeft = target.value === "true";
    this.setState({ candidatesLeft }, () => {
      this.props.onSettingsChange(this.state);
    });
    window.localStorage.setItem("candidatesLeft", candidatesLeft.toString());
  };

  componentDidMount() {
    const theme = window.localStorage.getItem("theme") || "auto";
    const candidatesLeft =
      (window.localStorage.getItem("candidatesLeft") || "true") === "true";
    applyTheme(theme);
    this.setState({ theme, candidatesLeft }, () => {
      this.props.onSettingsChange(this.state);
    });
  }

  render() {
    return (
      <SettingsComponentRoot>
        <SettingsButton onClick={() => this.props.onToggle()}>
          Settings
        </SettingsButton>
        {this.props.isOpen && (
          <SettingsComponentInnerContainer>
            <Label htmlFor="theme-selector">Theme</Label>
            <ThemeSelectorElm
              id="theme-selector"
              onChange={this.handleThemeChange}
              value={this.state.theme}
            >
              <option value="auto">System default</option>
              <option value="light">Light</option>
              <option value="dark">Dark</option>
            </ThemeSelectorElm>

            <Label htmlFor="candidates-position">
              Candidates Pane Position
            </Label>
            <CandidatesPositionElm
              id="candidates-position"
              onChange={this.handleCandidatesPositionChange}
              value={this.state.candidatesLeft.toString()}
            >
              <option value="true">Left</option>
              <option value="false">Right</option>
            </CandidatesPositionElm>

            <p>
              For instructions on how to use beancount-import,{" "}
              <a
                target="_blank"
                href="https://github.com/jbms/beancount-import/blob/master/README.md#usage"
              >
                please see the Readme
              </a>
              .
            </p>
          </SettingsComponentInnerContainer>
        )}
      </SettingsComponentRoot>
    );
  }
}
