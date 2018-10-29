import * as React from "react";
import styled from "styled-components";
import {
  ServerConnection,
  ServerState,
  WatchedFileHandle
} from "./server_connection";

import CodeMirror from "codemirror";

import "codemirror/lib/codemirror.css";
import "codemirror/addon/mode/simple";

import "codemirror/addon/selection/active-line";

// search
import "codemirror/addon/dialog/dialog";
import "codemirror/addon/search/searchcursor";
import "codemirror/addon/search/search";
import "codemirror/addon/dialog/dialog.css";

// folding
import "codemirror/addon/fold/foldcode";
import "codemirror/addon/fold/foldgutter";
import "./third_party/fava/codemirror/fold-beancount";
import "codemirror/addon/fold/foldgutter.css";

import "codemirror/addon/hint/show-hint";
import "codemirror/addon/hint/show-hint.css";
import "./third_party/fava/codemirror/hint-beancount";

import "./third_party/fava/codemirror/mode-beancount.js";

export interface SelectFileOptions {
  focus?: boolean;
  refresh?: boolean;
}

interface EditorComponentProps {
  serverConnection: ServerConnection;
  dirtyStateDidChange?: (dirty: boolean) => void;
  commonJournalPrefix: string;
}

interface EditorComponentState {
  selectedFilename?: string;
  journalFilenames?: string[];
  changedOnDisk: boolean;
  dirty: boolean;
  mainJournalPath?: string;
}

export class EditorComponent extends React.PureComponent<
  EditorComponentProps,
  EditorComponentState
> {
  private ref = React.createRef<HTMLDivElement>();

  state: EditorComponentState = { changedOnDisk: false, dirty: false };

  private fileHandle: WatchedFileHandle | undefined = undefined;
  private selectedLine: number | undefined = undefined;
  private cleanGeneration: number = -1;

  get selectedFilename() {
    const { fileHandle } = this;
    return fileHandle === undefined ? undefined : fileHandle.filename;
  }
  get cleanContents() {
    const { fileHandle } = this;
    return fileHandle === undefined ? undefined : fileHandle.contents;
  }

  private textEditor: CodeMirror.Editor = (() => {
    const textEditor = CodeMirror(
      element => {
        element.style.flex = "1";
      },
      {
        lineNumbers: true,
        foldGutter: true,
        styleActiveLine: true,
        mode: "beancount",
        gutters: ["CodeMirror-linenumbers", "CodeMirror-foldgutter"],
        readOnly: true,
        extraKeys: {
          Tab: "autocomplete",
          "Ctrl-S": () => {
            this.save();
          },
          "Cmd-S": () => {
            this.save();
          }
        }
      } as any
    );
    textEditor.on("changes", () => {
      this.setDirty();
    });
    textEditor.on("cursorActivity", () => {
      this.selectedLine = undefined;
    });
    textEditor.on("keyup", (cm: CodeMirror.Editor, event: Event) => {
      if (
        !cm.state.completionActive &&
        (event as KeyboardEvent).keyCode !== 13
      ) {
        (CodeMirror as any).commands.autocomplete(cm, null, {
          completeSingle: false
        });
      }
    });
    return textEditor;
  })();

  get isDirty() {
    if (this.cleanContents === undefined) {
      return false;
    }
    const doc = this.textEditor.getDoc();
    return !doc.isClean(this.cleanGeneration);
  }

  private setDirty() {
    const { dirtyStateDidChange } = this.props;
    if (dirtyStateDidChange !== undefined) {
      const dirty = this.isDirty;
      this.setState({ dirty });
      dirtyStateDidChange(dirty);
    }
  }

  private serverStateListener = (state: Partial<ServerState>) => {
    this.setState(s => {
      if (
        s.selectedFilename == null &&
        state.main_journal_path != null &&
        s.mainJournalPath == null
      ) {
        setTimeout(() => this.selectFile(state.main_journal_path), 0, false);
      }
      return {
        journalFilenames: state.journal_filenames,
        mainJournalPath: state.main_journal_path
      };
    });
  };

  private selectLine(line?: number) {
    if (line === undefined) {
      return;
    }
    this.textEditor.getDoc().setCursor({ line, ch: 0 });
    this.textEditor.scrollIntoView(null, 100);
    this.selectedLine = line;
  }

  selectFile(
    filename: string | undefined,
    line?: number,
    options: SelectFileOptions = {}
  ) {
    const { focus = true, refresh = false } = options;
    if (filename === "") {
      filename = undefined;
    }
    if (this.selectedFilename !== filename && this.isDirty) {
      return;
    }
    if (this.selectedFilename !== filename) {
      if (this.fileHandle !== undefined) {
        this.fileHandle.cancel();
        this.fileHandle = undefined;
      }
      const doc = this.textEditor.getDoc();
      this.textEditor.setValue("");
      doc.clearHistory();
      this.cleanGeneration = doc.changeGeneration();
      this.setDirty();
      this.textEditor.setOption("readOnly", true);
      this.setState({ selectedFilename: filename });
      if (filename !== undefined) {
        this.fileHandle = this.props.serverConnection.watchFile(
          filename,
          handle => {
            if (this.isDirty) {
              this.setState({ changedOnDisk: true });
            } else {
              this.textEditor.setOption("readOnly", false);
              const oldLine = this.selectedLine;
              const doc = this.textEditor.getDoc();
              const cursor = doc.getCursor();
              this.textEditor.setValue(handle.contents!);
              doc.setCursor(cursor);
              doc.clearHistory();
              this.cleanGeneration = doc.changeGeneration();
              this.setDirty();
              this.selectedLine = oldLine;
              if (!handle.needsUpdate) {
                const { selectedLine } = this;
                this.selectLine(this.selectedLine);
              }
            }
          }
        );
        if (refresh) {
          this.fileHandle!.refresh();
        }
        this.selectedLine = line;
      }
    } else {
      if (filename !== undefined) {
        this.selectedLine = line;
        if (refresh) {
          this.fileHandle!.refresh();
        } else {
          this.selectLine(line);
        }
      }
    }
    if (focus) {
      this.textEditor.focus();
    }
  }

  componentDidMount() {
    this.props.serverConnection.addListener(this.serverStateListener, true);
    this.fixTextEditor();
  }

  private fixTextEditor() {
    const wrapperElement = this.textEditor.getWrapperElement();
    const parentElement = this.ref.current!;
    if (wrapperElement !== parentElement.lastChild) {
      wrapperElement.style.flexBasis = "0px";
      parentElement.appendChild(wrapperElement);
      this.textEditor.refresh();
    }
  }

  componentDidUpdate() {
    this.fixTextEditor();
  }

  componentWillUnmount() {
    this.props.serverConnection.removeListener(this.serverStateListener);
    if (this.fileHandle !== undefined) {
      this.fileHandle.cancel();
      this.fileHandle = undefined;
    }
  }

  save = () => {
    if (this.cleanContents !== undefined) {
      this.props.serverConnection.send({
        type: "set_file_contents",
        value: {
          filename: this.selectedFilename!,
          contents: this.textEditor.getValue()
        }
      });
      this.cleanGeneration = this.textEditor.getDoc().changeGeneration();
      this.setDirty();
    }
  };

  revert = () => {
    if (this.cleanContents !== undefined) {
      if (this.textEditor.getValue() !== this.cleanContents) {
        this.textEditor.setValue(this.cleanContents);
        this.textEditor.getDoc().clearHistory();
      }
      this.cleanGeneration = this.textEditor.getDoc().changeGeneration();
      this.setDirty();
      this.setState({ changedOnDisk: false });
    }
  };

  render() {
    const { commonJournalPrefix } = this.props;
    return (
      <div
        ref={this.ref}
        style={{ display: "flex", flexDirection: "column", flex: 1 }}
      >
        <div
          style={{ display: "flex", flexDirection: "row", overflow: "hidden" }}
        >
          <button disabled={!this.state.dirty} onClick={this.save}>
            Save
          </button>
          <button disabled={!this.state.dirty} onClick={this.revert}>
            Revert
          </button>
          <select
            value={this.state.selectedFilename || ""}
            onChange={event => {
              this.selectFile(event.target.value);
            }}
          >
            <option key="" value="" />
            {(this.state.journalFilenames || []).map(filename => (
              <option key={filename} value={filename}>
                {filename.substring(commonJournalPrefix.length)}
              </option>
            ))}
          </select>
        </div>
        {this.state.changedOnDisk && (
          <div style={{ color: "red" }}>File has changed on disk</div>
        )}
      </div>
    );
  }
}
