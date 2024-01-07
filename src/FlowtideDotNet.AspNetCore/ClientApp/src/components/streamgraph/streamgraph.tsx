import React, { useEffect, useState } from 'react';
import {ReactFlow, Node, Position, Edge, Background, Controls} from 'reactflow';
import dagre from '@dagrejs/dagre';


// 

import 'reactflow/dist/style.css';
import { GraphNodeRender } from './noderender';

export interface StreamGraphNode {
    id: string,
    displayName: any
    counters?: Array<{ name: string, total: { rateLastMinute: number, sum: number} }>
    gauges?: Array<{ name: string, value: number }>
}

export interface StreamGraphEdge {
    source: string
    target: string
}

export interface StreamGraphProps {
    nodes: Array<StreamGraphNode>
    edges: Array<StreamGraphEdge>
}

const nodeTypes = {
    custom: GraphNodeRender
}

export const StreamGraph: React.FunctionComponent<StreamGraphProps> = (props: StreamGraphProps) => {

    const [nodes, setNodes] = useState<Node<any, string | undefined>[]>([])
    const [edges, setEdges] = useState<Edge<any>[]>([])
    
    useEffect(() => {

        const dagreGraph = new dagre.graphlib.Graph();
        dagreGraph.setDefaultEdgeLabel(() => ({}));

        const nodeWidth = 500;
        const nodeHeight = 100;
        
        dagreGraph.setGraph({ rankdir: "LR" });

        props.nodes.forEach((node) => {
            dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
        });

        props.edges.forEach((edge) => {
            dagreGraph.setEdge(edge.source, edge.target);
        });

        dagre.layout(dagreGraph);

        const outputNodes: Array<Node<any, string | undefined>> = []

        props.nodes.forEach((node) => {
            const nodeWithPosition = dagreGraph.node(node.id);

            const data: any = {
                label: node.id + ": " + node.displayName
            }

            if (node.counters) {
                const eventcounter = node.counters.find(x => x.name === "events")
                if(eventcounter) {
                    data.rateLastMinute = eventcounter.total.rateLastMinute
                }
            }
            if (node.gauges){
                const busy = node.gauges.find(x => x.name === "busy")
                if (busy) {
                    data.busy = busy.value;
                }
                const backpressure = node.gauges.find(x => x.name === "backpressure")
                if (backpressure){
                    data.backpressure = backpressure.value;
                }
            }

            const result: Node<any, string | undefined> = {
                id: node.id,
                targetPosition: Position.Left,
                sourcePosition: Position.Right,
                type: 'custom',
                position: {
                    x: nodeWithPosition.x - nodeWidth / 2,
                    y: nodeWithPosition.y - nodeHeight / 2,
                },
                data: data
            }

            outputNodes.push(result)
        })

        const outputEdges: Array<Edge<any>> = []

        props.edges.forEach((edge) => {
            outputEdges.push({
                id: `${edge.source}-${edge.target}`,
                source: edge.source,
                target: edge.target,
                animated: true,
                type: "straight"
            })
        })

        console.log(outputNodes)

        setNodes(outputNodes)
        setEdges(outputEdges)
    }, [props.nodes, props.edges])

    const proOptions = { hideAttribution: false };

    return (
        <div style={{height: '500px'}}>
        <ReactFlow 
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            proOptions={proOptions}
            fitView
        >
            <Background />
            <Controls />
        </ReactFlow>
        </div>
    )
}

