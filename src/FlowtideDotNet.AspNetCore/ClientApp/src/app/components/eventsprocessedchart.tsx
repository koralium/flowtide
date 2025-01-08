import { usePromRangeQuery } from "../hooks/usePromRangeQuery"
import { TimeChart } from "./timechart"
import { useGetTimeSpan } from "../hooks/useGetTimeSpan"
import { useChartJsData } from "../hooks/useChartJsData"




export const EventsProcessedChart = () => {
    const timespan = useGetTimeSpan()
    const memoryQuery = usePromRangeQuery("sum(rate(flowtide_events_processed_total[1s]))", timespan.start, timespan.end, "5s")
    const chartJsDataResult = useChartJsData([memoryQuery], (metric) => {
        return "Events processed per second"
    })

    return (
        <TimeChart height={200} width={"100%"} datasets={chartJsDataResult.result as any} />
    )
}