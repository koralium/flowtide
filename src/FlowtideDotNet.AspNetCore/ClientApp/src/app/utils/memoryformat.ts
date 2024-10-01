

export function memoryFormat(memory: number) {
    let unit = "b"
    if (memory > 1000) {
        unit = "kb"
        memory = memory / 1000
    }
    if (memory > 1000) {
        unit = "mb"
        memory = memory / 1000
    }
    if (memory > 1000) {
        unit = "gb"
        memory = memory / 1000
    }

    return (Math.round(memory * 100) / 100).toString() + unit

}