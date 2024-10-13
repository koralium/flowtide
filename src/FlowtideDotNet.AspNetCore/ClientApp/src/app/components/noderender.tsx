import React from 'react';
import { Handle, Position } from 'reactflow';

export interface GraphNodeRenderProps {
    data: {
        label: string
        eventsPerSecond?: number
        busy?: number
        backpressure?: number
    }
}

export const GraphNodeRender: React.FunctionComponent<GraphNodeRenderProps> = (props: GraphNodeRenderProps) => {

    let color = 'white dark:bg-gray-900';

    if (props.data.backpressure ?? 0 > 0.8) {
        color = 'bg-gray-400 dark:bg-gray-700'
    }
    if (props.data.busy ?? 0 > 0.8) {
        color = 'bg-red-400 dark:bg-red-700'
    }

    return (
        <div className={`border border-stone-400 p-5 dark:text-gray-200 shadow-md ${color}`}>
        <Handle type="target" position={Position.Left} isConnectable={false} />
        <div>
            {props.data.label}
            <br />
            {props.data.eventsPerSecond !== undefined ? `${props.data.eventsPerSecond} / s` : <></>}
            <br />
            {props.data.busy !== undefined ? `Busy: ${JSON.stringify(props.data.busy)}` : <></>}
            <br />
            {props.data.backpressure !== undefined ? `Backpressure: ${JSON.stringify(props.data.backpressure)}` : <></>}
        </div>
        <Handle type="source" position={Position.Right} isConnectable={false} />
        </div>
    )
}