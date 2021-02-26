import * as React from "react";
import styled from "styled-components";
import { UnclearedPosting } from "./server_connection";
import { AssociatedDataViewController, AssociatedDataViewContext } from "./app";
import { JournalLineReference } from "./invalid_references";
import { EventSubscription } from "fbemitter";
import {
  ServerVirtualListComponent,
  ServerVirtualListState
} from "./server_virtual_list";

class UnclearedVirtualListComponent extends ServerVirtualListComponent<
  UnclearedPosting
> {}

const UnclearedPostingsList = styled(UnclearedVirtualListComponent)`
  overflow-y: scroll;
  flex: 1;
`;

const UnclearedPostingElement = styled.div`
  cursor: pointer;
  padding: 12px 8px;
  border-bottom: 1px solid var(--color-main-accent);
  min-width: 100%;
  box-sizing: border-box;

  :hover {
    background-color: var(--color-hover-bg);
    color: var(--color-hover-text);
  }
`;

const UnclearedPostingFormattedElement = styled.div`
  font-family: var(--font-fam-mono);
  font-size: var(--font-size-mono-reg);
  white-space: pre;
`;

interface UnclearedPostingsComponentProps {
  listState: ServerVirtualListState<UnclearedPosting>;
}

interface UnclearedPostingComponentProps {
  entry: UnclearedPosting;
}

export class UnclearedPostingComponent extends React.PureComponent<
  UnclearedPostingComponentProps
> {
  private dataViewController?: AssociatedDataViewController;
  render() {
    const { entry } = this.props;
    const formattedText = entry.transaction_formatted;
    return (
      <AssociatedDataViewContext.Consumer>
        {dataViewController => {
          this.dataViewController = dataViewController;
          return (
            <UnclearedPostingElement onClick={this.handleClick}>
              <UnclearedPostingFormattedElement>
                {formattedText}
              </UnclearedPostingFormattedElement>
            </UnclearedPostingElement>
          );
        }}
      </AssociatedDataViewContext.Consumer>
    );
  }

  private handleClick = () => {
    const { dataViewController } = this;
    dataViewController!.selectFileByMeta(this.props.entry.posting.meta);
  };
}

export class UnclearedPostingsComponent extends React.PureComponent<
  UnclearedPostingsComponentProps
> {
  private renderItem(
    entry: UnclearedPosting,
    index: number,
    ref: React.RefObject<any>
  ) {
    return <UnclearedPostingComponent key={index} ref={ref} entry={entry} />;
  }

  render() {
    return (
      <UnclearedPostingsList
        listState={this.props.listState}
        renderItem={this.renderItem}
      />
    );
  }
}
