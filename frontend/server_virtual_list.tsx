import { EventEmitter, EventSubscription } from "fbemitter";
import * as React from "react";

import {
  ServerConnection,
  ServerDataType,
  ServerListCache,
  ServerState
} from "./server_connection";
import { VirtualList, VirtualListScrollState } from "./virtual_list";

type ServerListMetadata = [number, number];

export class ServerVirtualListState<T> {
  cache: ServerListCache<T>;
  scrollState = new VirtualListScrollState();
  metadata?: ServerListMetadata;
  emitter = new EventEmitter();

  private serverListener = (state: Partial<ServerState>) => {
    const value = state[this.dataType];
    if (value !== this.metadata) {
      this.metadata = value || undefined;
      this.emitter.emit("change", this.metadata);
    }
  };

  constructor(
    private serverConnection: ServerConnection,
    public dataType: ServerDataType
  ) {
    this.cache = new ServerListCache<T>(dataType);
    this.serverListener(serverConnection.state);
    this.serverConnection.addListener(this.serverListener);
  }

  dispose() {
    this.serverConnection.removeListener(this.serverListener);
  }
}

export interface ServerVirtualListComponentProps<T> {
  listState: ServerVirtualListState<T>;
  className?: string;
  renderItem: (item: T, index: number, ref: React.RefObject<any>) => any;
}

export interface ServerVirtualListComponentState {
  length: number;
  generation: number;
  availabilityGeneration: number;
}

export class ServerVirtualListComponent<T> extends React.PureComponent<
  ServerVirtualListComponentProps<T>,
  ServerVirtualListComponentState
> {
  state: ServerVirtualListComponentState = {
    length: 0,
    generation: -1,
    availabilityGeneration: 0
  };
  cacheSubscription?: EventSubscription;
  metadataSubscription?: EventSubscription;

  static getDerivedStateFromProps(
    props: ServerVirtualListComponentProps<any>
  ): Partial<ServerVirtualListComponentState> {
    const { metadata } = props.listState;
    if (metadata !== undefined) {
      return { generation: metadata[0], length: metadata[1] };
    }
    return {};
  }

  componentDidMount() {
    const { listState } = this.props;
    this.cacheSubscription = listState.cache.emitter.addListener(
      "received",
      (generation: number, startIndex: number, endIndex: number) => {
        this.setState({
          availabilityGeneration: this.state.availabilityGeneration + 1
        });
      }
    );
    this.metadataSubscription = listState.emitter.addListener(
      "change",
      (metadata?: ServerListMetadata) => {
        if (metadata !== undefined) {
          this.setState({ generation: metadata[0], length: metadata[1] });
        }
      }
    );
  }

  componentWillUnmount() {
    this.cacheSubscription!.remove();
    this.metadataSubscription!.remove();
  }

  private renderItem = (index: number, ref: React.RefObject<HTMLElement>) => {
    const entry = this.props.listState.cache.get(this.state.generation, index);
    if (entry === undefined) {
      return null;
    }
    return this.props.renderItem(entry, index, ref);
  };

  render() {
    return (
      <VirtualList
        length={this.state.length}
        itemSizeGeneration={this.state.generation}
        itemAvailabilityGeneration={this.state.availabilityGeneration}
        onRequestRange={this.onRequestRange}
        scrollState={this.props.listState.scrollState}
        className={this.props.className}
      >
        {(index: number, ref: React.RefObject<HTMLElement>) =>
          this.renderItem(index, ref)
        }
      </VirtualList>
    );
  }

  private onRequestRange = (startIndex: number, endIndex: number) => {
    const { listState } = this.props;
    listState.cache.requestRange(
      this.state.generation,
      this.state.length,
      startIndex,
      endIndex
    );
  };
}
