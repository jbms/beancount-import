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
  border: 0;
  background-color: transparent;
  color: inherit;
  cursor: pointer;
`;

const Label = styled.label`
  font-weight: bold;
  display: block;
  margin-bottom: 2px;
`;

const ThemeSelectorElm = styled.select`
  margin-bottom: 6px;
`;

interface SettingsComponentProps {
  isOpen: boolean;
  onToggle: (open?: boolean) => void;
}

interface SettingsComponentState {
  theme: string;
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

  window.localStorage.setItem("theme", theme);
}

export class SettingsComponent extends React.PureComponent<
  SettingsComponentProps,
  SettingsComponentState
> {
  state: SettingsComponentState = {
    theme: "auto"
  };

  private handleChange = ({ target }: React.ChangeEvent<HTMLSelectElement>) => {
    const theme = target.value;
    this.setState({ theme });
    applyTheme(theme);
  };

  componentDidMount() {
    const theme = window.localStorage.getItem("theme") || "auto";
    this.setState({ theme });
    applyTheme(theme);
  }

  render() {
    const ThemeSelector = (
      <ThemeSelectorElm>
        <option value="auto">System default</option>
        <option value="light">Light</option>
        <option value="dark">Dark</option>
      </ThemeSelectorElm>
    );

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
              onChange={this.handleChange}
              value={this.state.theme}
            >
              <option value="auto">System default</option>
              <option value="light">Light</option>
              <option value="dark">Dark</option>
            </ThemeSelectorElm>

            <p>
              For instructions on how to use beancount-import,{" "}
              <a
                target="_blank"
                href="https://github.com/jbms/beancount-import/blob/master/README.md#usage"
              >
                read the Readme
              </a>
              .
            </p>
          </SettingsComponentInnerContainer>
        )}
      </SettingsComponentRoot>
    );
  }
}
