import * as React from "react";
import * as ReactDOM from "react-dom";
import styled from "styled-components";
import { CandidateSelectionState } from "./candidates";
import {
  Candidate,
  getServerFileUrl,
  AssociatedEntryData
} from "./server_connection";
import { EventSubscription } from "fbemitter";

interface SourceDataComponentProps {
  candidateSelectionState: CandidateSelectionState;
}
interface SourceDataComponentState {
  selectedCandidate?: Candidate;
  selectedAssociatedData?: AssociatedEntryData;
  selectedAssociatedDataIndex?: number;
}
interface SourceDataListComponentProps {
  associatedData?: AssociatedEntryData[] | null;
  selectedIndex: number;
  onSelect: (index: number) => void;
}

function getIdentifier(data: AssociatedEntryData) {
  if (data.link != null) {
    return `^${data.link}`;
  }
  return `${data.meta![0]}: ${data.meta![1]}`;
}

const SourceDataList = styled.div``;

const SourceDataListItem = styled.div<{ selected: boolean }>`
  cursor: pointer;
  background-color: ${p => (p.selected ? "#fcc" : "transparent")};

  :hover {
    text-decoration: underline;
  }
`;

const SourceDataIdentifier = styled.span`
  font-weight: bold;
`;

const SourceDataDescription = styled.span`
  margin-left: 1em;
`;

class SourceDataListComponent extends React.PureComponent<
  SourceDataListComponentProps
> {
  render() {
    const { selectedIndex } = this.props;
    return (
      <SourceDataList>
        {(this.props.associatedData || []).map((data, i) => (
          <SourceDataListItem
            selected={i == selectedIndex}
            key={i}
            onClick={this.handleClick}
            data-id={i}
          >
            <SourceDataIdentifier>{getIdentifier(data)}</SourceDataIdentifier>
            <SourceDataDescription>{data.description}</SourceDataDescription>
          </SourceDataListItem>
        ))}
      </SourceDataList>
    );
  }

  private handleClick = (event: React.MouseEvent) => {
    const currentTarget = event.currentTarget as HTMLDivElement;
    const index = parseInt(currentTarget.dataset.id!);
    this.props.onSelect(index);
  };
}

export class SourceDataComponent extends React.PureComponent<
  SourceDataComponentProps,
  SourceDataComponentState
> {
  state: SourceDataComponentState = {};
  static getDerivedStateFromProps(
    props: SourceDataComponentProps,
    state: SourceDataComponentState
  ) {
    const {
      selectedCandidate,
      selectedAssociatedData,
      selectedAssociatedDataIndex
    } = props.candidateSelectionState;
    return {
      selectedCandidate,
      selectedAssociatedData,
      selectedAssociatedDataIndex
    };
  }

  private subscription?: EventSubscription;

  componentDidMount() {
    this.subscription = this.props.candidateSelectionState.emitter.addListener(
      "change",
      () => {
        const {
          selectedCandidate,
          selectedAssociatedData,
          selectedAssociatedDataIndex
        } = this.props.candidateSelectionState;
        this.setState({
          selectedCandidate,
          selectedAssociatedData,
          selectedAssociatedDataIndex
        });
      }
    );
  }

  componentWillUnmount() {
    this.subscription!.remove();
  }

  render() {
    const { selectedCandidate, selectedAssociatedData: data } = this.state;
    let dataElement: any;
    if (data !== undefined) {
      if (data.type.startsWith("image/")) {
        dataElement = (
          <img
            style={{ maxWidth: "100%" }}
            src={getServerFileUrl(data.path, data.type)}
          />
        );
      } else if (data.type === "text/html") {
        dataElement = (
          <iframe
            style={{ flex: "1" }}
            src={getServerFileUrl(data.path, data.type)}
            sandbox=""
          />
        );
      } else if (data.type === "application/pdf") {
        // Sandbox breaks pdf rendering.
        dataElement = (
          <iframe
            style={{ flex: "1" }}
            src={getServerFileUrl(data.path, data.type)}
          />
        );
      }
    }
    const associatedData =
      selectedCandidate == null ? [] : selectedCandidate.associated_data;
    return (
      <div style={{ flex: "1", display: "flex", flexDirection: "column" }}>
        <SourceDataListComponent
          associatedData={associatedData}
          selectedIndex={this.state.selectedAssociatedDataIndex || 0}
          onSelect={this.handleSelectData}
        />
        {dataElement}
      </div>
    );
  }

  private handleSelectData = (index: number) => {
    this.props.candidateSelectionState.setSelectedAssociatedDataItem(index);
  };
}
