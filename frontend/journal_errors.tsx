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
  margin: 0;
  padding-left: 3px;
  padding-right: 3px;
  overflow-y: scroll;
  flex: 1;
  flex-basis: 0px;
`;

const JournalErrorElement = styled.div`
  margin-top: 0;
  margin-bottom: 0;

  :hover {
    background-color: #ddd;
  }
`;

const JournalErrorFilename = styled.span`
  font-weight: bold;
`;

const JournalErrorLineNumber = styled.span`
  font-weight: bold;
`;

const JournalErrorMessage = styled.span`
  margin-left: 1em;
  color: red;
`;

const JournalLineReferenceElement = styled.span`
  cursor: pointer;
  :hover {
    background-color: #ddf;
  }
`;

interface JournalErrorsComponentProps {
  listState: ServerVirtualListState<JournalError>;
}

export class JournalLineReference extends React.PureComponent<{
  meta: { filename?: string | null; lineno?: number | null };
}> {
  render() {
    const { meta } = this.props;
    return (
      <AssociatedDataViewContext.Consumer>
        {dataViewController => (
          <CommonJournalPrefixContext.Consumer>
            {commonJournalPrefix => {
              return (
                <JournalLineReferenceElement
                  onClick={() => this.handleClick(dataViewController)}
                >
                  {meta.filename != null ? (
                    <JournalErrorFilename>
                      {meta.filename.substring(commonJournalPrefix.length)}
                    </JournalErrorFilename>
                  ) : (
                    undefined
                  )}
                  {meta.lineno !== undefined ? (
                    <JournalErrorLineNumber>
                      :{meta.lineno}
                    </JournalErrorLineNumber>
                  ) : (
                    undefined
                  )}
                </JournalLineReferenceElement>
              );
            }}
          </CommonJournalPrefixContext.Consumer>
        )}
      </AssociatedDataViewContext.Consumer>
    );
  }

  private handleClick = (dataViewController: AssociatedDataViewController) => {
    dataViewController.selectFileByMeta(this.props.meta);
  };
}

export class JournalErrorComponent extends React.PureComponent<{
  error: JournalError;
  associatedDataViewController: AssociatedDataViewController;
  commonJournalPrefix: string;
}> {
  render() {
    const { error, commonJournalPrefix } = this.props;
    const [severity, message, meta] = error;
    return (
      <JournalErrorElement onClick={this.handleErrorClick}>
        {meta.filename !== undefined ? (
          <JournalErrorFilename>
            {meta.filename.substring(commonJournalPrefix.length)}
          </JournalErrorFilename>
        ) : (
          undefined
        )}
        {meta.lineno !== undefined ? (
          <JournalErrorLineNumber>:{meta.lineno}:</JournalErrorLineNumber>
        ) : (
          undefined
        )}
        <JournalErrorMessage>
          {severity}: {message}
        </JournalErrorMessage>
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
