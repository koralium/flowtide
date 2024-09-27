import { useEffect, useState } from "react";
import { Metric, PromRangeQueryResult } from "./usePromRangeQuery";


export const useChartJsData = (queries: Array<PromRangeQueryResult>, labelFunc: (metric: Metric) => string) => {

   // const [result, setResult] = useState<any>()

   

   for (let i = 0; i < queries.length; i++) {
        if (queries[i].loading) {
            return {
                result: [],
                loading: true
            };
        }
    }

    const dataSets = queries.flatMap(x => {
        const dataArray = x.result.flatMap(y => {
            
            const d = y.values.map(z => {
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
    // useEffect(() => {

        //dataSets[0].data[0]

    //     setResult(dataSets)

    // }, [queries])
    
    return {
        result: dataSets,
        loading: false
    }
}