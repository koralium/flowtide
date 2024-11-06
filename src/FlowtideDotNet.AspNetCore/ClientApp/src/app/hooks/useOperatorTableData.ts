import { useGetTimeSpan } from "./useGetTimeSpan"
import { useJoinInstantQueries } from "./useJoinInstantQueries"
import { usePromInstantQuery } from "./usePromRangeQuery"

export interface OperatorTableRow {
    id: string
    name: string
    totalEvents: number
    busy: number
    backpressure: number
    memoryUsage: number
}

export interface OperatorTableResult {
    result: Array<OperatorTableRow>
    loading: boolean
    error: string | undefined
}

/**
 * Fetches all data required for the operator table
 */
export const useOperatorTableData = (): OperatorTableResult => {

    const timespan = useGetTimeSpan()
    const metadataQuery = usePromInstantQuery("flowtide_metadata", timespan.end)
    const totalEventsQuery = usePromInstantQuery("flowtide_events_total", timespan.end)
    const busyQuery = usePromInstantQuery("flowtide_busy", timespan.end)
    const backpressureQuery = usePromInstantQuery("flowtide_backpressure", timespan.end)
    const memoryUsageQuery = usePromInstantQuery("flowtide_memory_allocated_bytes - flowtide_memory_freed_bytes", timespan.end)

    // Join all the data
    const joinResult = useJoinInstantQueries(
        [metadataQuery, totalEventsQuery, busyQuery, backpressureQuery, memoryUsageQuery], 
        "operator",
        {
            "operator": "id",
            "title": "name"
        },
        {
            1: "totalEvents",
            2: "busy",
            3: "backpressure",
            4: "memoryUsage"
        })
    
    return {
        result: (joinResult.result as any) as Array<OperatorTableRow>,
        error: joinResult.error,
        loading: joinResult.loading
    }
}