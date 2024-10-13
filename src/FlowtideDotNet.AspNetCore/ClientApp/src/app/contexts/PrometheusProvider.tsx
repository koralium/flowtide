"use client"
import { PrometheusDriver } from "prometheus-query"
import { createContext } from "react"

export const PromContext = createContext<PrometheusDriver>({} as any);

export interface PrometheusProviderProps {
    url: string
    baseUrl: string
    children: React.ReactNode
} 

export const PrometheusProvider = (props: PrometheusProviderProps) => {

    const prom = new PrometheusDriver({
        endpoint: props.url,
        baseURL: props.baseUrl // default value
    });
    
    return (
        <PromContext.Provider value={prom}>
            {props.children}
        </PromContext.Provider>
    )
}