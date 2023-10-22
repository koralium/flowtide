
export interface CardProps {
    children?: React.ReactNode
}

export const Card = (props: CardProps) => {
    return (
        <div className="mt-2 p-6 bg-white border border-gray-200 rounded-lg shadow dark:bg-gray-800 dark:border-gray-700">
            {props.children}
        </div>
    )
}