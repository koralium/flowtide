import { useContext } from "react"
import { TimespanContext } from "../contexts/TimespanProvider"


export interface TimeSpan {
    start: number
    end: number
}

export const useGetTimeSpan = (): TimeSpan => {
    const context = useContext(TimespanContext)

    return context
}