import { usePromRangeQuery } from "../hooks/usePromRangeQuery"
import { TimeChart } from "./timechart"
import { useGetTimeSpan } from "../hooks/useGetTimeSpan"
import { useChartJsData } from "../hooks/useChartJsData"




export const CacheHitsChart = () => {
    const timespan = useGetTimeSpan()
    const memoryQuery = usePromRangeQuery("avg(rate(flowtide_lru_table_cache_hits_total[1s])/rate(flowtide_lru_table_cache_tries_total[1s]))", timespan.start, timespan.end, "5s")
    const chartJsDataResult = useChartJsData([memoryQuery], (metric) => {
        return "Cache Hits %"
    })

    return (
        <TimeChart 
            height={400} 
            width={600} 
            datasets={chartJsDataResult.result as any} 
            formatValue={(v) => (v * 100).toString() + "%"}
            />
    )
}