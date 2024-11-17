export interface StreamGraphNode {
    id: string,
    displayName: any
    busy: number
    backpressure: number
    eventsPerSecond: number
}

export interface StreamGraphEdge {
    source: string
    target: string
}