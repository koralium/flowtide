import { useMemo } from "react";

export interface StreamStatusProps {
    status: string
    align?: "left" | "right"
}

const getTextAndStyles = (status: string, align?: "left" | "right"): {styles: string, text: string} => {
    let styles = "";
    let text = "";
    switch(status.toLowerCase()) {
        case "notstarted":
            styles = "bg-white border border-gray-300"
            text = "Not Started"
            break;
        case "running":
            styles = "text-white bg-green-700"
            text = "Running"
            break;
        case "starting":
            styles = "focus:outline-none text-white bg-yellow-500"
            text = "Starting"
            break;
        case "failure":
            styles = "text-white bg-red-700"
            text = "Failure"
            break;
        default:
            styles = "text-white bg-red-700"
            text = status
            break;
    }

    if (align === "right") {
        styles = styles + " ml-auto mr-0";
    }

    return {styles, text};
};

export const StreamStatus = (props: StreamStatusProps) => {
    const textAndStyles = useMemo(() => getTextAndStyles(props.status, props.align), [props.status, props.align])

    return (
        <div className={`${textAndStyles.styles} max-w-52 font-medium rounded-lg text-sm px-5 py-4`}>{textAndStyles.text}</div>
    )
}