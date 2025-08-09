import { usePromRangeQuery } from "../hooks/usePromRangeQuery"
import { TimeChart } from "./timechart"
import { useGetTimeSpan } from "../hooks/useGetTimeSpan"
import { useChartJsData } from "../hooks/useChartJsData"




export const DiskCacheWriteChart = () => {
    const timespan = useGetTimeSpan()
    const memoryQuery = usePromRangeQuery("avg(rate(flowtide_temporary_write_ms_sum[1s])/rate(flowtide_temporary_write_ms_count[1s]) > 0)", timespan.start, timespan.end, "5s")
    const chartJsDataResult = useChartJsData([memoryQuery], (metric) => {
        return "Cache Write To Disk Ms"
    })

    return (
        <TimeChart 
            height={400} 
            width={"100%"} 
            datasets={chartJsDataResult.result as any} 
            formatValue={(v) => v.toString() + " ms"}
            />
    )
}