"use client"
import { createContext, useContext, useEffect, useState } from "react"

export interface TimeProviderData {
    start: number,
    end: number
}

export const TimespanContext = createContext<TimeProviderData>({} as any);

export interface TimespanProviderProps {
    // defaultStart?: Date,
    // defaultEnd?: Date
    // type: 'relative' | 'absolute'
    // relativeTimeMs?: number
    // relativeRefreshRateMs?: number
    children: React.ReactNode
} 

export interface TimeSpanUpdaterProps {
    children: React.ReactNode
    relativeTimeMs: number
    relativeRefreshRateMs: number
}

const TimeSpanUpdater = (props: TimeSpanUpdaterProps) => {
    const context = useContext(TimespanContext)
    

     useEffect(() => {
        const currentTime = Date.now()
        context.start = currentTime - props.relativeTimeMs;
        context.end = currentTime;
        setInterval(() => {
            const currentTime = Date.now()
            context.start = currentTime - props.relativeTimeMs;
            context.end = currentTime;
        }, props.relativeRefreshRateMs)
    })

    return (
        <>{props.children}</>
    )
}

const TimespanProviderInternal = (props: TimespanProviderProps & TimeSpanUpdaterProps) => {
    
    const [timeData, setTimeData] = useState<TimeProviderData>({start: Date.now().valueOf() - props.relativeTimeMs, end: Date.now().valueOf()})

    useEffect(() => {
        const currentTime = Date.now()
        const newstart = currentTime - props.relativeTimeMs;
        const newend = currentTime;
        setTimeData({start: newstart, end: newend})
        setInterval(() => {
            const currentTime = Date.now()
            const newstart = currentTime - props.relativeTimeMs;
            const newend = currentTime;
            setTimeData({start: newstart, end: newend})
        }, props.relativeRefreshRateMs)
    }, 
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [])

    return (
        <TimespanContext.Provider value={timeData}>
            {props.children}
        </TimespanContext.Provider>
    )
}

export const TimespanProvider = (props: TimespanProviderProps & TimeSpanUpdaterProps) => {

    return (
        <TimespanProviderInternal {...props}>
            {/* <TimeSpanUpdater relativeRefreshRateMs={props.relativeRefreshRateMs} relativeTimeMs={props.relativeTimeMs}> */}
                {props.children}
            {/* </TimeSpanUpdater> */}
        </TimespanProviderInternal>
    )
}