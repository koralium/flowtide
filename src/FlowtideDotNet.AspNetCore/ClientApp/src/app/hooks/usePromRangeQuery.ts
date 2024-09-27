import { useContext, useEffect, useState } from "react"
import { PrometheusQueryDate, SampleValue } from 'prometheus-query';
import { PromContext } from "../contexts/PrometheusProvider";

export interface Metric {
    name: string
    labels: { [key: string] : string }
}

export interface RangeQueryResult {
    metric: Metric
    values: Array<SampleValue>
}

export interface InstantQueryData {
    metric: Metric
    value: SampleValue
}

export interface InstantQueryResult {
    result: Array<InstantQueryData>
    loading: boolean
    error: string | undefined
}

export const usePromInstantQuery = (query: string, time?: PrometheusQueryDate) : InstantQueryResult => {
    const driver = useContext(PromContext)
    const [result, setResult] = useState<Array<InstantQueryData>| undefined>(undefined)
    const [error, setError] = useState(undefined)

    //debugger

    useEffect(() => {
        driver.instantQuery(query, time)
        .then((res) => {
            setResult(res.result as any)
        })
        .catch((err) => {
            setError(err)
        })
    }, [query, time])

    if (error) {
        return { result: result as any, loading: false, error };
    }
    if (result === undefined){
        return { result: result as any, loading: true, error };
    }
    return { result, loading: false, error };
}

export interface PromRangeQueryResult {
    result: Array<RangeQueryResult>
    loading: boolean 
    error: string | undefined
}

export const usePromRangeQuery = (query: string, start: PrometheusQueryDate, end: PrometheusQueryDate, step: string | number) : PromRangeQueryResult => {
    const driver = useContext(PromContext)
    const [result, setResult] = useState<Array<RangeQueryResult>| undefined>(undefined)
    const [error, setError] = useState(undefined)
    
    useEffect(() => {
        driver.rangeQuery(query, start, end, step)
        .then((res) => {
            setResult(res.result as any)
        })
        .catch((err) => {
            setError(err)
        })
    }, [query, start, end, step])

    if (error) {
        return { result: result as any, loading: false, error };
    }
    if (result === undefined){
        return { result: result as any, loading: true, error };
    }
    
    return { result, loading: false, error };
}