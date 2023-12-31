﻿#pragma warning disable IDE0073 // The file header does not match the required text
// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
#pragma warning restore IDE0073 // The file header does not match the required text

// =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
//
// IDebuggerDisplay.cs
//
//
// An interface implemented by objects that expose their debugger display
// attribute content through a property, making it possible for code to query
// for the same content.
//
// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

namespace DataflowStream.dataflow.Internal
{
    /// <summary>Implemented to provide customizable data for debugger displays.</summary>
    internal interface IDebuggerDisplay
    {
        /// <summary>The object to be displayed as the content of a DebuggerDisplayAttribute.</summary>
        /// <remarks>
        /// The property returns an object to allow the debugger to interpret arbitrary .NET objects.
        /// The return value may be, but need not be limited to be, a string.
        /// </remarks>
        object Content { get; }
    }
}