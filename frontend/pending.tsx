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
`;

const PendingEntryElement = styled<
  { selected: boolean; highlighted: boolean, isOdd: boolean },
  "div"
>("div")`
  border: 1px solid ${props =>
    props.highlighted ? "black" : "transparent"};
  ${props => (props.selected ? "border-color: blue;" : "")}
  position: relative;
  font-family: "Helvetica Neue", HelveticaNeue, Helvetica, Arial, sans-serif;
  font-size: 12px;
  background-color: ${props =>
    props.isOdd ? "transparent" : "rgba(0, 0, 0, 0.05)"};
  padding: 12px 6px;
`;

const PendingEntryFormattedElement = styled.div`
  font-family: Source Code Pro, Courier New, Courier, monospace;
  white-space: pre;
  font-size: 13px;
  margin: 0 0 8px;
`;

const PendingEntrySourceNameElement = styled.div`
  margin: 0 0 8px;
`;

const PendingEntrySourceFilenameElement = styled.div`
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
        isOdd={this.props.index % 2 === 0}
        onMouseEnter={this.handleMouseEnter}
        onMouseLeave={this.handleMouseLeave}
      >
        <PendingEntryFormattedElement>
          {entry.formatted.trim()}
        </PendingEntryFormattedElement>
        <PendingEntrySourceNameElement>
          <em>Source:</em> {source}
        </PendingEntrySourceNameElement>
        {filename && (
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
