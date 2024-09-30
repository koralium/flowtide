import { usePromRangeQuery } from "../hooks/usePromRangeQuery"
import { TimeChart } from "./timechart"
import { useGetTimeSpan } from "../hooks/useGetTimeSpan"
import { useChartJsData } from "../hooks/useChartJsData"
import { memoryFormat } from "../utils/memoryformat"




export const MemoryUsageChart = () => {
    const timespan = useGetTimeSpan()
    const memoryQuery = usePromRangeQuery("sum(flowtide_memory_allocated_bytes - flowtide_memory_freed_bytes)", timespan.start, timespan.end, "5s")
    const chartJsDataResult = useChartJsData([memoryQuery], (metric) => {
        return "Unmanaged Memory Usage"
    })

    return (
        <TimeChart 
            height={400} 
            width={"100%"} 
            datasets={chartJsDataResult.result as any} 
            formatValue={(v) => memoryFormat(v)}
            />
    )
}