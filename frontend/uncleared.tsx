import * as React from "react";
import styled from "styled-components";
import { UnclearedPosting } from "./server_connection";
import { AssociatedDataViewController, AssociatedDataViewContext } from "./app";
import { JournalLineReference } from "./journal_errors";
import { EventSubscription } from "fbemitter";
import {
  ServerVirtualListComponent,
  ServerVirtualListState
} from "./server_virtual_list";

class UnclearedVirtualListComponent extends ServerVirtualListComponent<
  UnclearedPosting
> {}

const UnclearedPostingsList = styled(UnclearedVirtualListComponent)`
  margin: 0;
  padding-left: 3px;
  padding-right: 3px;
  overflow-y: scroll;
  flex: 1;
  flex-basis: 0px;
`;

const UnclearedPostingElement = styled.div`
  border: 1px solid transparent;
  margin-top: 0;
  margin-bottom: 0;
  padding-top: 3px;
  padding-bottom: 3px;

  :hover {
    border: 1px solid black;
  }
`;

const UnclearedPostingFormattedElement = styled.div`
  font-family: monospace;
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
