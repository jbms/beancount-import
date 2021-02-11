import * as React from "react";
import * as ReactDOM from "react-dom";
import styled from "styled-components";
import scrollIntoView from "scroll-into-view-if-needed";
import { PendingEntry, ServerListCache } from "./server_connection";
import { VirtualList, VirtualListScrollState } from "./virtual_list";
import { EventEmitter, EventSubscription } from "fbemitter";
import {
  ServerVirtualListComponent,
  ServerVirtualListState
} from "./server_virtual_list";

class PendingVirtualListComponent extends ServerVirtualListComponent<
  PendingEntry
> {}

const PendingEntryListElement = styled(PendingVirtualListComponent)`
  overflow-y: scroll;
  flex: 1;
  flex-basis: 0px;
  background-color: var(--color-primary-bg);
  padding: 8px 0;
`;

const PendingEntryElement = styled<
  { selected: boolean; highlighted: boolean; },
  "div"
>("div")`
  cursor: pointer;
  font-size: var(--font-size-sans-small);
  padding: 12px 9px;
  border-bottom: 1px solid #ccc;
  min-width: 100%;
  box-sizing: border-box;
  ${props => (props.highlighted && 
    `
    background-color: var(--color-pending-highlight-bg);
    color: var(--color-pending-highlight-text);
    `
  )};
  ${props => (props.selected && 
    `
    background-color: var(--color-pending-select-bg);
    color: var(--color-main-text-on-select);
    `
  )};
`;

// background-color: ${props => (props.selected ? "transparent" : "rgba(0, 0, 0, 0.1)")};

const PendingEntryFormattedElement = styled.div`
  font-family: var(--font-fam-mono);
  font-size: var(--font-size-mono-reg);
  white-space: pre;
`;

const PendingEntrySourceNameElement = styled.div`
  border-top: 1px solid #fff;
  margin: 6px 0 2px;
  padding: 6px 0 0px;
  white-space: nowrap;
`;

const PendingEntrySourceFilenameElement = styled.div`
  white-space: nowrap;
`;

const PendingEntryInfoElement = styled.div`
  text-align: center;
`;

export class PendingEntryHighlightState {
  emitter = new EventEmitter();
  index?: number = undefined;

  set(index: number) {
    if (index !== this.index) {
      this.index = index;
      this.emitter.emit("set");
    }
  }
}

class PendingEntryComponent extends React.PureComponent<{
  entry: PendingEntry;
  selected: boolean;
  highlighted: boolean;
  index: number;
  onSelect: (index: number) => void;
  onHover: (index?: number) => void;
}> {
  render() {
    const { entry } = this.props;
    let filename: string | undefined;
    let lineno: number | undefined;
    let source = entry.source;
    if (source != null) {
      if (entry.info != null && entry.info.filename != null) {
        filename = entry.info.filename;
        if (entry.info.line != null) {
          lineno = entry.info.line;
        }
      }
    } else {
      source = "fixme";
      const meta = entry.entries[0].meta;
      if (meta != null && meta["filename"] != null) {
        filename = meta["filename"];
        if (meta["lineno"] != null) {
          lineno = meta["lineno"];
        }
      }
    }
    return (
      <PendingEntryElement
        onClick={this.handleSelect}
        selected={this.props.selected}
        highlighted={this.props.highlighted}
        onMouseEnter={this.handleMouseEnter}
        onMouseLeave={this.handleMouseLeave}
      >
        <PendingEntryFormattedElement>
          {entry.formatted.trim()}
        </PendingEntryFormattedElement>
        {this.props.selected && (
          <PendingEntrySourceNameElement>
            <em>Source:</em> {source}
          </PendingEntrySourceNameElement>
        )}
        {this.props.selected && filename && (
          <PendingEntrySourceFilenameElement>
            <em>File:</em> {filename}
            {lineno != undefined && `:${lineno}`}
          </PendingEntrySourceFilenameElement>
        )}
      </PendingEntryElement>
    );
  }

  private handleMouseEnter = () => {
    this.props.onHover(this.props.index);
  };

  private handleMouseLeave = () => {
    this.props.onHover(undefined);
  };

  private handleSelect = () => {
    this.props.onSelect(this.props.index);
  };
}

interface PendingEntriesComponentProps {
  listState: ServerVirtualListState<PendingEntry>;
  onSelect: (index: number) => void;
  selectedIndex?: number;
  highlightState: PendingEntryHighlightState;
}

interface PendingEntriesComponentState {
  highlightedIndex?: number;
}

export class PendingEntriesComponent extends React.PureComponent<
  PendingEntriesComponentProps,
  PendingEntriesComponentState
> {
  state: PendingEntriesComponentState = {
    highlightedIndex: this.props.highlightState.index
  };

  selectedRef = React.createRef<HTMLElement>();
  highlightedRef = React.createRef<HTMLElement>();

  private renderItem = (
    entry: PendingEntry,
    index: number,
    ref: React.RefObject<any>
  ) => {
    const { selectedIndex } = this.props;
    const { highlightedIndex } = this.state;
    return (
      <PendingEntryComponent
        selected={index === selectedIndex}
        key={index}
        ref={ref}
        entry={entry}
        index={index}
        onSelect={this.props.onSelect}
        onHover={this.handleHover}
        highlighted={index === highlightedIndex}
      />
    );
  };

  render() {
    const { selectedIndex } = this.props;
    const { highlightedIndex } = this.state;
    // Use renderItem.bind(this) to force re-render of VirtualList whenever we re-render.
    return (
      <PendingEntryListElement
        listState={this.props.listState}
        renderItem={this.renderItem.bind(this)}
      />
    );
  }

  highlightStateSubscription?: EventSubscription;
  componentDidMount() {
    this.highlightStateSubscription = this.props.highlightState.emitter.addListener(
      "set",
      () => {
        this.setState({ highlightedIndex: this.props.highlightState.index });
      }
    );
  }

  componentWillUnmount() {
    this.highlightStateSubscription!.remove();
  }

  private handleHover = (index?: number) => {
    this.setState({ highlightedIndex: index });
  };
}
