import { useJoinInstantQueries } from "./useJoinInstantQueries"
import { usePromInstantQuery } from "./usePromRangeQuery"
import { useGetTimeSpan } from "./useGetTimeSpan"
import { StreamGraphNode, StreamGraphEdge } from "../types/streamtypes"


export interface StreamGraphNodeResult {
    nodes: Array<StreamGraphNode>
    edges: Array<StreamGraphEdge>
    loading: boolean
    error: string | undefined
    // id: string
    // name: string
    // eventsPerSecond: number
    // busy: number
    // backpressure: number
}

export const useStreamGraphData = (): StreamGraphNodeResult => {
    const timespan = useGetTimeSpan()
    
    // Node queries
    const metadataQuery = usePromInstantQuery("flowtide_metadata", timespan.end)
    const busyQuery = usePromInstantQuery("flowtide_busy", timespan.end)
    const backpressureQuery = usePromInstantQuery("flowtide_backpressure", timespan.end)
    const eventsPerSecondQuery = usePromInstantQuery("rate(flowtide_events_processed_total[1s])", timespan.end)

    // Edge queries
    const linksQuery = usePromInstantQuery("flowtide_link", timespan.end)

    const joinResult = useJoinInstantQueries(
        [metadataQuery, eventsPerSecondQuery, busyQuery, backpressureQuery], 
        "operator",
        {
            "operator": "id",
            "title": "displayName"
        },
        {
            1: "eventsPerSecond",
            2: "busy",
            3: "backpressure"
        })

    const linksResult = useJoinInstantQueries([linksQuery],"operator", {
        "source": "source",
        "target": "target"
    }, {})

    if (joinResult.loading || linksResult.loading) {
        return { nodes: [], edges: [], loading: true, error: undefined };
    }
    if (joinResult.error) {
        return { nodes: [], edges: [], loading: false, error: joinResult.error };
    }
    if (linksResult.error) {
        return { nodes: [], edges: [], loading: false, error: linksResult.error };
    }

    return {
        nodes: (joinResult.result as any) as Array<StreamGraphNode>,
        edges: (linksResult.result as any) as Array<StreamGraphEdge>,
        loading: false,
        error: undefined
    }
}