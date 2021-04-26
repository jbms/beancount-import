import * as React from "react";
import styled from "styled-components";
import { JournalError } from "./server_connection";
import {
  AssociatedDataViewController,
  AssociatedDataViewContext,
  CommonJournalPrefixContext
} from "./app";
import {
  ServerVirtualListComponent,
  ServerVirtualListState
} from "./server_virtual_list";

class ErrorsVirtualListComponent extends ServerVirtualListComponent<
  JournalError
> {}

const JournalErrorList = styled(ErrorsVirtualListComponent)`
  overflow-y: scroll;
  flex: 1;
`;

const JournalErrorElement = styled.div`
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

const JournalErrorMessage = styled.div`
  line-height: 1.4;
`;

const JournalErrorSource = styled.div`
  font-size: var(--font-size-sans-small);
  border-top: 1px solid var(--color-main-accent);
  margin: 6px 0 2px;
  padding: 6px 0 0px;
  white-space: nowrap;
`;

interface JournalErrorsComponentProps {
  listState: ServerVirtualListState<JournalError>;
}

export class JournalErrorComponent extends React.PureComponent<{
  error: JournalError;
  associatedDataViewController: AssociatedDataViewController;
  commonJournalPrefix: string;
}> {
  render() {
    const { error, commonJournalPrefix } = this.props;
    const [severity, message, meta] = error;
    const { filename, lineno } = meta;
    return (
      <JournalErrorElement onClick={this.handleErrorClick}>
        <JournalErrorMessage>
          <strong>{severity}</strong>:<br />
          {message}
        </JournalErrorMessage>
        {filename && (
          <JournalErrorSource>
            <em>File:</em> {filename.substring(commonJournalPrefix.length)}
            {lineno !== undefined && `:${lineno}`}
          </JournalErrorSource>
        )}
      </JournalErrorElement>
    );
  }
  private handleErrorClick = (event: React.MouseEvent<HTMLElement>) => {
    const index = parseInt(event.currentTarget.dataset.index!);
    this.props.associatedDataViewController.selectFileByMeta(
      this.props.error[2]
    );
  };
}

export class JournalErrorsComponent extends React.PureComponent<
  JournalErrorsComponentProps
> {
  private renderItem(
    entry: JournalError,
    index: number,
    ref: React.RefObject<any>
  ) {
    return (
      <AssociatedDataViewContext.Consumer key={index}>
        {dataViewController => (
          <CommonJournalPrefixContext.Consumer>
            {commonJournalPrefix => (
              <JournalErrorComponent
                ref={ref}
                error={entry}
                associatedDataViewController={dataViewController}
                commonJournalPrefix={commonJournalPrefix}
              />
            )}
          </CommonJournalPrefixContext.Consumer>
        )}
      </AssociatedDataViewContext.Consumer>
    );
  }

  render() {
    return (
      <JournalErrorList
        listState={this.props.listState}
        renderItem={this.renderItem}
      />
    );
  }
}
