import { Chart,
    TimeSeriesScale,
    TimeScale, //Import timescale instead of category for X axis
    LinearScale,
    PointElement,
    LineElement,
    Title,
    Tooltip,
    Legend
 } from "chart.js/auto";
 import 'chartjs-adapter-date-fns';
 import { enUS } from 'date-fns/locale';
import { useEffect, useRef, useState } from "react";
import { useGetTimeSpan } from "../hooks/useGetTimeSpan";


Chart.register(
    TimeSeriesScale,
    TimeScale, //Register timescale instead of category for X axis
    LinearScale,
    PointElement,
    LineElement,
    Title,
    Tooltip,
    Legend
  );

export interface TimeChartProps {
    datasets: Array<{data: Array<{x: number, y: number}>, label: string}>
    height: string | number
    width: string | number
    formatValue?: (value: number) => string
}

export const TimeChart = (props: TimeChartProps) => {

    const timespan = useGetTimeSpan()
    const currentChartRef = useRef<Chart>()
    const chartRef = useRef<HTMLCanvasElement>(null);
    const chartId = useRef<string | null>(null);

    const interval = (timespan.end - timespan.start) / 10;

    
    useEffect(() => {
        if (chartRef.current === null) {
            return;
        }
        if (chartId.current !== null) {
          return;
        }

        const labels: Date[] = []
        for (let i = 0; i < 10; i++) {
            labels.push(new Date(timespan.start + (interval * i)))
        }

        const chart = new Chart(chartRef.current, {
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: {
                    easing: 'linear',
                    y: {
                        duration: 0 
                    }
                 } as any,
                elements: {
                    point: {
                        radius: 0,
                        hitRadius: 10,
                        hoverRadius: 10
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function (value, index, values) {
                                if (props.formatValue) {
                                    return props.formatValue(value as any)
                                }

                                return value;
                            }
                        }
                    },
                    x: {
                        type: 'time',
                        ticks: {
                            source: 'labels'
                        },
                        adapters: {
                            date: {
                                locale: enUS
                            }
                        }
                    }
                },
                plugins: {
                    tooltip: {
                        callbacks: {
                           label: function(t: any, d: any) {
                                if (props.formatValue) {
                                    return props.formatValue(t.raw.y)
                                }
                              return t.formattedValue;
                           }
                        }
                     }
                } as any
            },
            data: {
                labels: labels,
                datasets: props.datasets
            },
            type: "line",
            plugins: {
                tooltip: {
                    callbacks: {
                       label: function(t: any, d: any) {
                            if (props.formatValue) {
                                return props.formatValue(t.raw.y)
                            }
                          return t.formattedValue;
                       }
                    }
                 }
            } as any
        });
        currentChartRef.current = chart;
        
        chartId.current = chart.id;
        return () => {
            if (chartId.current !== null){
                Chart.getChart(chartId.current)?.destroy()
            }
        };
      }, 
      // eslint-disable-next-line react-hooks/exhaustive-deps
      []);

    useEffect(() => {
        
        if (currentChartRef.current === null) {
            return;
        }
        const chart = currentChartRef.current

        
        if (chart) {
            const labels: Date[] = []
            for (let i = 0; i < 10; i++) {
                labels.push(new Date(timespan.start + (interval * i)))
            }
            labels.push(new Date(timespan.end))
            
            for (let i = 0; i < props.datasets.length; i++) {
                
                const updatedSet = props.datasets[i]
                const existing = chart.data.datasets.find(x => x.label == props.datasets[i].label) as any

                if (existing === undefined) {
                    chart.data.datasets.push(updatedSet)
                    continue;
                }

                const existingData = existing.data as Array<{x: number, y: number}>;
                if (updatedSet.data.length > 0) {
                    const minTime = updatedSet.data[0].x
                    
                    let currentMaxTime = 0;
                    if (existingData.length > 0) {
                        currentMaxTime = existingData[existingData.length - 1].x;
                        for (let k = 0; k < existingData.length; k++) {
                            if (existingData[k].x < minTime) {
                                existingData.shift()
                            }
                            else {
                                break;
                            }
                        }
                    }
                    for (let k = 0; k < updatedSet.data.length; k++) {
                        if (updatedSet.data[k].x > currentMaxTime) {
                            existingData.push(updatedSet.data[k])
                        }
                    }
                }
                else {
                    existingData.length = 0;
                }
                

            }

            // for (let i = 0; i < chart.data.datasets.length; i++) {
            //     chart.data.datasets[i].
            // }
            // //chart.opt
            // chart.data.datasets = props.datasets
            chart.data.labels = labels
            chart.update();
        }
    }, 
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [props.datasets])

    return (
        <div className="chart-container" style={
            {
                position: 'relative',
                height: props.height,
                width: props.width
            }
        }>
        <canvas ref={chartRef}></canvas>
        </div>
    )
}