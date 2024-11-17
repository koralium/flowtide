import { usePromRangeQuery } from "../hooks/usePromRangeQuery"
import { TimeChart } from "./timechart"
import { useGetTimeSpan } from "../hooks/useGetTimeSpan"
import { useChartJsData } from "../hooks/useChartJsData"




export const PageCountChart = () => {
    const timespan = useGetTimeSpan()
    const currentUsageQuery = usePromRangeQuery("flowtide_lru_table_size", timespan.start, timespan.end, "5s")
    const maxUsageQuery = usePromRangeQuery("flowtide_lru_table_max_size", timespan.start, timespan.end, "5s")
    const cleanupStartQuery = usePromRangeQuery("flowtide_lru_table_cleanup_start", timespan.start, timespan.end, "5s")
    const chartJsDataResult = useChartJsData([currentUsageQuery, maxUsageQuery, cleanupStartQuery], (metric) => {
        switch(metric.name){
            case "flowtide_lru_table_size":
                return "Current Page Count"
            case "flowtide_lru_table_max_size":
                return "Max Page Count"
            case "flowtide_lru_table_cleanup_start":
                return "Cleanup Start Page Count"
        }
        return metric.name
    })

    return (
        <TimeChart height={400} width={"100%"} datasets={chartJsDataResult.result as any} />
    )
}