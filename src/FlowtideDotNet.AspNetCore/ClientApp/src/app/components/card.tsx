import React from "react"

export interface CardProps {
    children?: React.ReactNode
}

export const Card = (props: CardProps) => {
    return (
        <div className="mt-2 p-6 mr-1 ml-1 border border-gray-200 rounded-lg shadow dark:border-gray-700">
            {props.children}
        </div>
    )
}