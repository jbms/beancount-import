import * as React from "react";
import styled from "styled-components";
import { UsedTransaction } from "./server_connection";
import { AssociatedDataViewController, AssociatedDataViewContext } from "./app";

const UsedTransactionList = styled.div`
  border-bottom: 1px solid var(--color-main-accent);
`;

const UsedTransactionElement = styled.div<
  { selected: boolean; hover: boolean }>`
  font-family: var(--font-fam-mono);
  background-color: var(--color-main-bg);
  color: var(--color-main-text);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  line-height: 24px;

  ${props => (props.hover && 
    `
    background-color: var(--color-hover-bg);
    color: var(--color-hover-text);
    `
  )};
  ${props => (props.selected && 
    `
    background-color: var(--color-select-bg);
    color: var(--color-select-text);
    `
  )};
`;

const UsedTransactionCheckbox = styled.input<{disabled: boolean}>`
  width: 16px;
  height: 16px;
  vertical-align: middle;
  padding: 0px;
  margin: 0 6px;
  display: inline-block;
  cursor: ${props => props.disabled ? 'default' : 'pointer'};
`;

export class UsedTransactionsComponent extends React.PureComponent<{
  usedTransactions: UsedTransaction[];
  disabledUsedTransactions: Set<number>;
  selectedUsedTransactions: number[];
  hoverUsedTransactions: number[];
  onChange: (index: number, value: boolean) => void;
}> {
  private handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const { currentTarget } = event;
    this.props.onChange(
      parseInt(currentTarget.dataset.id!),
      currentTarget.checked
    );
  };

  dataViewController?: AssociatedDataViewController;

  render() {
    const { disabledUsedTransactions } = this.props;
    const selectedIndices = new Set(this.props.selectedUsedTransactions);
    const hoverIndices = new Set(this.props.hoverUsedTransactions);
    return (
      <AssociatedDataViewContext.Consumer>
        {dataViewController => {
          this.dataViewController = dataViewController;
          return (
            <UsedTransactionList>
              {this.props.usedTransactions.map((usedTransaction, index) => {
                const enabled = !disabledUsedTransactions.has(index);
                return (
                  <UsedTransactionElement
                    key={index}
                    selected={selectedIndices.has(index)}
                    hover={hoverIndices.has(index)}
                    title="Show this pending transaction in the list of pending transactions."
                    onClick={this.handleClick}
                    data-id={index}
                  >
                    <UsedTransactionCheckbox
                      data-id={index}
                      disabled={index === 0}
                      type="checkbox"
                      checked={enabled}
                      onChange={this.handleChange}
                      title={
                        index === 0
                          ? "All candidates include this pending transaction."
                          : `${
                              enabled ? "Exclude" : "Include"
                            } candidates containing this pending transaction.`
                      }
                    />
                    {usedTransaction.formatted.split("\n")[0]}
                  </UsedTransactionElement>
                );
              })}
            </UsedTransactionList>
          );
        }}
      </AssociatedDataViewContext.Consumer>
    );
  }

  private handleClick = (event: React.MouseEvent<HTMLElement>) => {
    if (event.target instanceof HTMLInputElement) {
      /// Don't select the pending entry if the user has clicked on the checkbox.
      return;
    }
    const index = parseInt(
      (event.currentTarget.firstChild as HTMLElement).dataset.id!
    );
    const entry = this.props.usedTransactions[index];
    if (entry.pending_index != null) {
      this.dataViewController!.highlightPending(entry.pending_index);
    } else {
      this.dataViewController!.selectFileByMeta(entry.entry.meta);
    }
  };
}
