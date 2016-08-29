import * as React from "react";
import throttle from "lodash/throttle";
import debounce from "lodash/debounce";
import * as ReactDOM from "react-dom";
import { default as Measure, ContentRect } from "react-measure";
import scrollIntoView from "scroll-into-view-if-needed";
import { EventEmitter, EventSubscription } from "fbemitter";

export class VirtualListScrollState {
  static readonly INDEX_REQUESTED = "indexRequested";
  emitter = new EventEmitter();
  generation = 0;
  anchorIndex: number = 0;

  /** Position of anchorIndex boundary relative to top of viewport.
   */
  anchorScrollOffset: number = 0;

  /**
   * If specified, requests that the list be scrolled to display the specified index.
   */
  requestedIndex?: number;

  scrollToIndex(index: number) {
    this.requestedIndex = index;
    this.emitter.emit(VirtualListScrollState.INDEX_REQUESTED);
  }
}

type ItemRenderer = (index: number, ref: React.RefObject<any>) => any;
export interface VirtualListProps {
  // If props are changed, shouldComponentUpdate must also be updated.
  length: number;
  children: ItemRenderer;
  scrollContainerProps?: React.HTMLAttributes<HTMLDivElement>;
  className?: string;
  itemSizeGeneration?: any;
  itemAvailabilityGeneration?: any;
  onRequestRange?: (startIndex: number, endIndex: number) => void;
  scrollState: VirtualListScrollState;
}

const overRenderFraction = 0.5;
const defaultNumItemsToRender = 10;
const maxAnchorError = 50;
const updateDelay = 20;

function guessRenderRequest(renderAnchorIndex: number, length: number) {
  const requestedRenderStartIndex = Math.max(
    0,
    renderAnchorIndex - Math.floor(defaultNumItemsToRender / 2)
  );
  const requestedRenderEndIndex = Math.min(
    length,
    requestedRenderStartIndex + defaultNumItemsToRender
  );
  return {
    renderAnchorIndex,
    requestedRenderStartIndex,
    requestedRenderEndIndex
  };
}

export class VirtualList extends React.PureComponent<VirtualListProps> {
  private renderRequestInitialized = false;
  private renderAnchorIndex = 0;
  private renderAnchorPixel = 0;
  private requestedRenderStartIndex = 0;
  private requestedRenderEndIndex = 0;

  // Set by render.
  private renderedChildrenRefs: React.RefObject<any>[] = [];
  private spacerRef = React.createRef<HTMLDivElement>();
  private topChildrenRef = React.createRef<HTMLDivElement>();
  private bottomChildrenRef = React.createRef<HTMLDivElement>();
  private actualRenderStartIndex = 0;
  private actualRenderEndIndex = 0;

  // Computed by afterRender.
  private itemSizeGeneration: any;
  private itemSizes: number[] = [];
  private totalSize = 0;
  private numItemsInTotal = 0;
  private totalSpecifiedHeight = 0;
  private averageItemSize = 0;
  private scrollStateGeneration = -1;

  componentDidUpdate() {
    this.afterRender();
  }

  private scrollStateSubscription?: EventSubscription;

  private setRenderRange(options: {
    renderAnchorIndex: number;
    renderAnchorPixel: number;
    requestedRenderStartIndex: number;
    requestedRenderEndIndex: number;
  }) {
    let forceUpdate = false;
    if (
      options.renderAnchorIndex !== this.renderAnchorIndex ||
      options.renderAnchorPixel !== this.renderAnchorPixel ||
      options.requestedRenderStartIndex !== this.requestedRenderStartIndex ||
      options.requestedRenderEndIndex !== this.requestedRenderEndIndex
    ) {
      // FIXME: take into account actualRenderStartIndex, actualRenderEndIndex
      forceUpdate = true;
    }
    const { onRequestRange } = this.props;
    if (onRequestRange !== undefined) {
      onRequestRange(
        options.requestedRenderStartIndex,
        options.requestedRenderEndIndex
      );
    }
    this.renderAnchorIndex = options.renderAnchorIndex;
    this.renderAnchorPixel = options.renderAnchorPixel;
    this.requestedRenderStartIndex = options.requestedRenderStartIndex;
    this.requestedRenderEndIndex = options.requestedRenderEndIndex;
    return forceUpdate;
  }

  componentDidMount() {
    this.scrollStateSubscription = this.props.scrollState.emitter.addListener(
      VirtualListScrollState.INDEX_REQUESTED,
      () => this.handleScrollRequest()
    );
    this.afterRender();
    this.handleScrollRequest();
  }

  private handleScrollRequest() {
    const { scrollState } = this.props;
    const { requestedIndex } = scrollState;
    if (requestedIndex === undefined) {
      return;
    }
    const { length } = this.props;
    if (requestedIndex < 0 || requestedIndex >= length) {
      scrollState.requestedIndex = undefined;
      return;
    }
    const { actualRenderStartIndex, actualRenderEndIndex } = this;
    if (
      requestedIndex >= actualRenderStartIndex &&
      requestedIndex < actualRenderEndIndex
    ) {
      const component = this.renderedChildrenRefs[
        requestedIndex - this.requestedRenderStartIndex
      ].current!;
      const node = ReactDOM.findDOMNode(component) as HTMLElement;
      scrollIntoView(node, {
        behavior: "instant",
        scrollMode: "if-needed",
        block: "nearest"
      });
      const scrollContainer = ReactDOM.findDOMNode(this) as HTMLElement;
      if (scrollContainer != null) {
        scrollContainer.scrollLeft = 0;
      }
      return;
    }

    const position = this.getRelativeBoundaryPosition(requestedIndex, 0, 0);
    scrollState.anchorIndex = requestedIndex;
    scrollState.anchorScrollOffset = 0;
    ++scrollState.generation;
    if (this.setViewport(requestedIndex, position, position)) {
      this.forceUpdate();
    }
  }

  private getItemSize(index: number) {
    const size = this.itemSizes[index];
    return size === undefined ? this.averageItemSize : size;
  }

  private getRelativeBoundaryPosition(
    index: number,
    baseIndex: number,
    basePosition: number
  ) {
    for (; baseIndex < index; ++baseIndex) {
      basePosition += this.getItemSize(baseIndex);
    }
    for (; index < baseIndex; ++index) {
      basePosition -= this.getItemSize(index);
    }
    return basePosition;
  }

  private afterRender() {
    const scrollContainer = ReactDOM.findDOMNode(this) as HTMLElement;
    if (scrollContainer == null) return;
    const { itemSizes } = this;
    const { renderedChildrenRefs } = this;
    const { actualRenderStartIndex, actualRenderEndIndex } = this;
    const { requestedRenderStartIndex } = this;
    const { length } = this.props;
    let numItemsInTotal = this.numItemsInTotal;
    let totalSize = this.totalSize;
    const scrollContainerBounds = scrollContainer.getBoundingClientRect();
    const { scrollTop } = scrollContainer;
    const basePixelOffset = scrollTop - scrollContainerBounds.top;
    // Update itemSizes.
    for (
      let childIndex = actualRenderStartIndex;
      childIndex < actualRenderEndIndex;
      ++childIndex
    ) {
      const component =
        renderedChildrenRefs[childIndex - requestedRenderStartIndex].current;
      if (component === null) continue;
      const node = ReactDOM.findDOMNode(component) as HTMLElement | null;
      if (node === null) continue;
      const bounds = node.getBoundingClientRect();
      const newSize = bounds.height;
      const existingSize = itemSizes[childIndex];
      if (existingSize !== undefined) {
        totalSize -= existingSize;
        --numItemsInTotal;
      }
      itemSizes[childIndex] = newSize;
      ++numItemsInTotal;
      totalSize += newSize;
    }
    this.totalSize = totalSize;
    this.numItemsInTotal = numItemsInTotal;
    if (numItemsInTotal === 0) return;
    const averageItemSize = (this.averageItemSize =
      totalSize / numItemsInTotal);
    const totalSpecifiedHeight = (this.totalSpecifiedHeight =
      averageItemSize * this.props.length);
    const spacer = this.spacerRef.current;
    if (spacer !== null) {
      // fixme: debounce this
      spacer.style.height = totalSpecifiedHeight + "px";
    }

    // check anchor position
    let { renderAnchorIndex } = this;
    let renderAnchorPixel = this.renderAnchorPixel;
    const expectedAnchorPixel = this.getRelativeBoundaryPosition(
      renderAnchorIndex,
      0,
      0
    );
    const anchorOffset = expectedAnchorPixel - renderAnchorPixel;
    if (
      this.scrollStateGeneration !== this.props.scrollState.generation ||
      Math.abs(anchorOffset) > maxAnchorError ||
      (anchorOffset !== 0 &&
        (actualRenderStartIndex === 0 || actualRenderEndIndex == length))
    ) {
      let newScrollPosition: number;
      // if expected anchor position is
      if (this.scrollStateGeneration !== this.props.scrollState.generation) {
        newScrollPosition =
          this.getRelativeBoundaryPosition(
            this.props.scrollState.anchorIndex,
            0,
            0
          ) - this.props.scrollState.anchorScrollOffset;
      } else {
        newScrollPosition = scrollTop + anchorOffset;
      }
      this.scrollStateGeneration = this.props.scrollState.generation;
      renderAnchorPixel = expectedAnchorPixel;
      scrollContainer.scrollTop = newScrollPosition;
      const topChildren = this.topChildrenRef.current;
      const bottomChildren = this.bottomChildrenRef.current;

      if (topChildren !== null) {
        topChildren.style.bottom = `calc(100% - ${renderAnchorPixel}px)`;
      }

      if (bottomChildren !== null) {
        bottomChildren.style.top = `${renderAnchorPixel}px`;
      }
      this.renderAnchorPixel = renderAnchorPixel;
    }
    this.maybeUpdateWindow();
  }

  private setViewport(
    renderAnchorIndex?: number,
    renderAnchorPixel?: number,
    scrollTop?: number
  ) {
    const { length } = this.props;
    const { state } = this;
    if (renderAnchorIndex === undefined) {
      renderAnchorIndex = this.renderAnchorIndex;
    }
    if (renderAnchorPixel === undefined) {
      renderAnchorPixel = this.renderAnchorPixel;
    }
    const scrollContainer = ReactDOM.findDOMNode(this) as HTMLElement;
    if (
      scrollContainer == null ||
      scrollContainer.offsetWidth === 0 ||
      scrollContainer.offsetHeight === 0 ||
      this.numItemsInTotal === 0
    ) {
      // Just use heuristic.
      if (
        renderAnchorPixel !== this.renderAnchorPixel ||
        renderAnchorIndex !== this.renderAnchorIndex
      ) {
        return this.setRenderRange({
          renderAnchorPixel,
          ...guessRenderRequest(renderAnchorIndex, length)
        });
      }
      return false;
    }
    const { clientHeight } = scrollContainer;
    if (scrollTop === undefined) {
      scrollTop = scrollContainer.scrollTop;
    }
    const { itemSizes } = this;
    const scrollBottom = scrollTop + clientHeight;
    const overRenderAmount = overRenderFraction * clientHeight;
    const maxRenderStartPixel = scrollTop - overRenderAmount;
    const minRenderStartPixel = scrollTop - 2 * overRenderAmount;
    const minRenderEndPixel = scrollBottom + overRenderAmount;
    const maxRenderEndPixel = scrollBottom + 2 * overRenderAmount;
    let { requestedRenderStartIndex, requestedRenderEndIndex } = this;
    // minIndex is inclusive, maxIndex is include.
    const adjustIndexToSatisfyPositionConstraints = (
      index: number,
      minIndex: number,
      maxIndex: number,
      minPosition: number,
      maxPosition: number
    ) => {
      let position = this.getRelativeBoundaryPosition(
        index,
        renderAnchorIndex!,
        renderAnchorPixel!
      );
      for (; position < minPosition && index + 1 <= maxIndex; ++index) {
        const itemSize = this.getItemSize(index);
        if (position + itemSize >= maxPosition) break;
        position += itemSize;
      }
      for (; position >= maxPosition && index > minIndex; --index) {
        const itemSize = this.getItemSize(index - 1);
        if (position - itemSize < minPosition) break;
        position -= itemSize;
      }
      return { index, position };
    };

    const adjustStartIndexToSatisfyPositionConstraints = (
      index: number,
      minIndex: number,
      maxIndex: number,
      minPosition: number,
      maxPosition: number
    ) => {
      let position = this.getRelativeBoundaryPosition(
        index,
        renderAnchorIndex!,
        renderAnchorPixel!
      );
      if (position < minPosition) {
        for (; index + 1 <= maxIndex; ++index) {
          const itemSize = this.getItemSize(index);
          if (position + itemSize >= maxPosition) break;
          position += itemSize;
        }
      } else if (position >= maxPosition) {
        for (; position > minPosition && index > minIndex; --index) {
          const itemSize = this.getItemSize(index - 1);
          position -= itemSize;
        }
      }
      return { index, position };
    };

    const adjustEndIndexToSatisfyPositionConstraints = (
      index: number,
      minIndex: number,
      maxIndex: number,
      minPosition: number,
      maxPosition: number
    ) => {
      let position = this.getRelativeBoundaryPosition(
        index,
        renderAnchorIndex!,
        renderAnchorPixel!
      );
      if (position < minPosition) {
        for (; position <= maxPosition && index + 1 <= maxIndex; ++index) {
          const itemSize = this.getItemSize(index);
          position += itemSize;
        }
      } else if (position >= maxPosition) {
        for (; index > minIndex; --index) {
          const itemSize = this.getItemSize(index - 1);
          if (position - itemSize < minPosition) break;
          position -= itemSize;
        }
      }
      return { index, position };
    };
    const newAnchor = adjustIndexToSatisfyPositionConstraints(
      renderAnchorIndex,
      0,
      length,
      minRenderStartPixel,
      maxRenderEndPixel
    );
    const newRenderStart = adjustStartIndexToSatisfyPositionConstraints(
      requestedRenderStartIndex,
      0,
      renderAnchorIndex,
      minRenderStartPixel,
      maxRenderStartPixel
    );
    const newRenderEnd = adjustEndIndexToSatisfyPositionConstraints(
      requestedRenderEndIndex,
      renderAnchorIndex,
      length,
      minRenderEndPixel,
      maxRenderEndPixel
    );
    return this.setRenderRange({
      requestedRenderStartIndex: newRenderStart.index,
      renderAnchorIndex: newAnchor.index,
      renderAnchorPixel: newAnchor.position,
      requestedRenderEndIndex: newRenderEnd.index
    });
  }

  private maybeUpdateWindow = throttle(
    () => {
      if (this.setViewport()) {
        this.forceUpdate();
      }
    },
    updateDelay,
    { trailing: true }
  );

  render() {
    const {
      scrollContainerProps: origScrollContainerProps = {},
      itemSizeGeneration
    } = this.props;
    if (itemSizeGeneration !== this.itemSizeGeneration) {
      this.itemSizeGeneration = itemSizeGeneration;
      this.itemSizes.length = 0;
      this.numItemsInTotal = 0;
      this.totalSize = 0;
      this.totalSpecifiedHeight = 0;
      this.renderRequestInitialized = false;
    }
    if (!this.renderRequestInitialized) {
      this.renderRequestInitialized = true;
      const { scrollState } = this.props;
      let renderAnchorIndex: number,
        renderAnchorPixel: number,
        scrollTop: number;
      if (scrollState.requestedIndex !== undefined) {
        renderAnchorIndex = scrollState.requestedIndex;
        renderAnchorPixel = 0;
      } else {
        renderAnchorIndex = scrollState.anchorIndex;
        renderAnchorPixel = scrollState.anchorScrollOffset;
      }
      this.setRenderRange({
        renderAnchorPixel,
        ...guessRenderRequest(renderAnchorIndex, this.props.length)
      });
    }
    let {
      style: scrollContainerStyle = {},
      ...scrollContainerProps
    } = origScrollContainerProps;

    const { totalSpecifiedHeight } = this;
    scrollContainerStyle = { ...scrollContainerStyle, position: "relative" };

    const {
      requestedRenderStartIndex,
      requestedRenderEndIndex,
      renderAnchorIndex,
      renderAnchorPixel
    } = this;
    const numRenderedChildren =
      requestedRenderEndIndex - requestedRenderStartIndex;
    const { renderedChildrenRefs } = this;
    for (let i = renderedChildrenRefs.length; i < numRenderedChildren; ++i) {
      renderedChildrenRefs[i] = React.createRef();
    }
    const topChildren: any[] = [];
    const bottomChildren: any[] = [];
    const renderItem = this.props.children;
    let actualRenderStartIndex = renderAnchorIndex;
    for (
      let index = renderAnchorIndex - 1;
      index >= requestedRenderStartIndex;
      --index
    ) {
      const renderResult = renderItem(
        index,
        renderedChildrenRefs[index - requestedRenderStartIndex]
      );
      if (renderResult == null) break;
      topChildren.push(renderResult);
      actualRenderStartIndex = index;
    }
    let actualRenderEndIndex = renderAnchorIndex;
    for (
      let index = renderAnchorIndex;
      index < requestedRenderEndIndex;
      ++index
    ) {
      const renderResult = renderItem(
        index,
        renderedChildrenRefs[index - requestedRenderStartIndex]
      );
      if (renderResult == null) break;
      bottomChildren.push(renderResult);
      actualRenderEndIndex = index + 1;
    }
    this.actualRenderStartIndex = actualRenderStartIndex;
    this.actualRenderEndIndex = actualRenderEndIndex;
    topChildren.reverse();
    return (
      <Measure bounds onResize={this.maybeUpdateWindow}>
        {({ measureRef }) => (
          <div
            ref={measureRef}
            className={this.props.className}
            {...scrollContainerProps}
            style={scrollContainerStyle}
            onScroll={this.handleScroll}
          >
            <div
              style={{
                position: "absolute",
                zIndex: -1,
                top: 0,
                left: 0,
                height: totalSpecifiedHeight,
                width: 1
              }}
              ref={this.spacerRef}
            />
            {topChildren.length > 0 ? (
              <div
                style={{
                  position: "absolute",
                  left: 0,
                  bottom: `calc(100% - ${renderAnchorPixel}px)`
                }}
                ref={this.topChildrenRef}
              >
                {topChildren}
              </div>
            ) : (
              undefined
            )}
            {bottomChildren.length > 0 ? (
              <div
                style={{
                  position: "absolute",
                  left: 0,
                  top: renderAnchorPixel
                }}
                ref={this.bottomChildrenRef}
              >
                {bottomChildren}
              </div>
            ) : (
              undefined
            )}
          </div>
        )}
      </Measure>
    );
  }

  private handleScroll = (event: React.UIEvent<HTMLDivElement>) => {
    const scrollContainer = ReactDOM.findDOMNode(this) as HTMLElement;
    if (
      scrollContainer == null ||
      scrollContainer.offsetWidth === 0 ||
      scrollContainer.offsetHeight === 0
    ) {
      return;
    }
    const { scrollState } = this.props;
    scrollState.anchorIndex = this.renderAnchorIndex;
    this.scrollStateGeneration = ++scrollState.generation;
    scrollState.anchorScrollOffset =
      this.renderAnchorPixel - scrollContainer.scrollTop;
    scrollState.requestedIndex = undefined;
    this.maybeUpdateWindow();
  };

  componentWillUnmount() {
    this.scrollStateSubscription!.remove();
    this.maybeUpdateWindow.cancel();
    const { onRequestRange } = this.props;
    if (onRequestRange !== undefined) {
      onRequestRange(0, 0);
    }
  }
}
