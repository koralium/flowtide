import React from "react"
import { StreamGraphNode } from "./streamgraph/streamgraph"

export interface NodeTableProps {
    nodes: Array<StreamGraphNode>
}


export const NodeTable = (props: NodeTableProps) => {

    const rows = props.nodes.map(x => {
        const eventcounter = x.counters?.find(y => y.name === "flowtide_events")

        let events = 0;
        if (eventcounter) {
            events = eventcounter.total.sum
        }

        let busyValue = 0
        let backpressureValue = 0
        const busy = x.gauges?.find(y => y.name === "flowtide_busy")
        if (busy) {
            busyValue = busy.value;
        }
        const backpressure = x.gauges?.find(y => y.name === "flowtide_backpressure")
        if (backpressure){
            backpressureValue = backpressure.value;
        }

        return (
            <tr key={x.id} className="bg-white border-b dark:bg-gray-800 dark:border-gray-700">
                <th scope="row" className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
                    {x.id}
                </th>
                <td className="px-6 py-4">
                    {x.displayName}
                </td>
                <td className="px-6 py-4">
                    {events}
                </td>
                <td className="px-6 py-4">
                    {busyValue}
                </td>
                <td className="px-6 py-4">
                    {backpressureValue}
                </td>
            </tr>
        )
    })

    return (
        <div className="relative overflow-x-auto">
            <table className="w-full text-sm text-left text-gray-500 dark:text-gray-400">
                <thead className="text-xs text-gray-700 uppercase bg-gray-50 dark:bg-gray-700 dark:text-gray-400">
                    <tr>
                        <th scope="col" className="px-6 py-3">
                            Id
                        </th>
                        <th scope="col" className="px-6 py-3">
                            Name
                        </th>
                        <th scope="col" className="px-6 py-3">
                            Total Events
                        </th>
                        <th scope="col" className="px-6 py-3">
                            Busy
                        </th>
                        <th scope="col" className="px-6 py-3">
                            Backpressure
                        </th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        </div>
    )
}