import { useGetTimeSpan } from "../hooks/useGetTimeSpan"
import { useOperatorTableData } from "../hooks/useOperatorTableData"
import { memoryFormat } from "../utils/memoryformat"


export const OperatorTable = () => {

    const {result, loading, error } = useOperatorTableData()

    if (error) {
        return (
            <div>{error}</div>
        )
    }

    const rows = result.map(x => {
        return (
            <tr key={x.id} className="border-b dark:border-gray-700">
                <th scope="row" className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
                    {x.id}
                </th>
                <td className="px-6 py-4">
                    {x.name}
                </td>
                <td className="px-6 py-4">
                    {x.totalEvents}
                </td>
                <td className="px-6 py-4">
                    {x.busy}
                </td>
                <td className="px-6 py-4">
                    {x.backpressure}
                </td>
                <td className="px-6 py-4">
                    {memoryFormat(x.memoryUsage)}
                </td>
            </tr>
        )
    })

    return (
        <div className="relative overflow-x-auto">
            <table className="w-full text-sm text-left text-gray-500 dark:text-gray-400">
                <thead className="text-xs text-gray-700 uppercase bg-gray-50 dark:bg-slate-900 dark:text-gray-400">
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
                        <th scope="col" className="px-6 py-3">
                            Memory Usage
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