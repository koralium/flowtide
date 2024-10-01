import { useEffect, useState } from "react";
import { Metric, PromRangeQueryResult } from "./usePromRangeQuery";


export const useChartJsData = (queries: Array<PromRangeQueryResult>, labelFunc: (metric: Metric) => string) => {

   const [result, setResult] = useState<any>([])
   const [lastStartEnd, setLastStartEnd] = useState({start: 0, end: 0})

   useEffect(() => {

    for (let i = 0; i < queries.length; i++) {
        if (queries[i].loading) {
            return;
        }
    }

    let currentstart = Number.MAX_SAFE_INTEGER;
    let currentend = 0;

    const dataSets = queries.flatMap(x => {
        const dataArray = x.result.flatMap(y => {
            
            const d = y.values.map(z => {
                if (z.time.valueOf() < currentstart) {
                    currentstart = z.time.valueOf()
                }
                if (z.time.valueOf() > currentend) {
                    currentend = z.time.valueOf()
                }
                return {
                    x: z.time.valueOf(),
                    y: z.value
                }
            })
            const label = labelFunc(y.metric)
            return {
                label,
                data: d
            }
        });
        return dataArray
    
    })

    if (currentstart !== lastStartEnd.start || currentend !== lastStartEnd.end) {
        setLastStartEnd({start: currentstart, end: currentend})
        setResult(dataSets)
    }
    
   }, [queries])
    
    return {
        result: result,
        loading: false
    }
}