import { useEffect, useState } from "react";
import { InstantQueryResult } from "./usePromRangeQuery";

export interface JoinResult {
    result: Array<{ [key: string]: string | number }>
    loading: boolean
    error: string | undefined
}

export const useJoinInstantQueries = (
    queries: Array<InstantQueryResult>, 
    labelToJoinOn: string,
    labelsToKeepAndRename: { [key: string]: string},
    valueIdsToKeepAndRename: { [key: number]: string}): JoinResult => 
{

    const [lastUsedDate, setLastUsedDate] = useState(0)
    const [finalResult, setFinalResult] = useState<Array<{ [key: string]: string | number }>>([])

    useEffect(() => {

        
        for (const element of queries) {
            if (element.loading) {
                return
            }
            if (element.error) {
                return
            }
        }

        const map: { [key: string]: any} = {}

    let lastDate: number = 0;
    
    for (let q = 0; q < queries.length; q++) {
        const res = queries[q].result
 
        for (let i = 0; i < res.length; i++) {
            const row = res[i]
            const joinValue = row.metric.labels[labelToJoinOn]

            let mapValue = map[joinValue]
            if (mapValue === undefined) {
                mapValue = {}
                map[joinValue] = mapValue
            }
            for (const [key, value] of Object.entries(labelsToKeepAndRename)) {
                
                const labelToKeep = row.metric.labels[key]

                if (labelToKeep) {
                    mapValue[value] = labelToKeep
                }
            }
            lastDate = row.value.time.valueOf()
            for (const [key, value] of Object.entries(valueIdsToKeepAndRename)) {
                
                if (key == q.toString()) {
                    mapValue[value] = row.value.value
                }
            }
        }
    }

    const output: Array<{ [key: string]: string | number }> = []


    for (const [key, value] of Object.entries(map)) {
        // Set default values for missing values
        for (const [labelKey, labelVal] of Object.entries(labelsToKeepAndRename)) {
            if (value[labelVal] === undefined) {
                value[labelVal] = ""
            }
        }
        for (const [valueKey, valueVal] of Object.entries(valueIdsToKeepAndRename)) {
            if (value[valueVal] === undefined) {
                value[valueVal] = 0
            }
        }
        output.push(value)
    }

    if (lastDate !== lastUsedDate){
        setLastUsedDate(lastDate)
        setFinalResult(output)
    }

    }, 
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [queries])

    return {
        result: finalResult,
        loading: false,
        error: undefined
    }
}