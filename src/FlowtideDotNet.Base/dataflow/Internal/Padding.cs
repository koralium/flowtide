#pragma warning disable IDE0073 // The file header does not match the required text
// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
#pragma warning restore IDE0073 // The file header does not match the required text

using System.Runtime.InteropServices;

namespace DataflowStream.dataflow.Internal
{
    /// <summary>A placeholder class for common padding constants and eventually routines.</summary>
    internal static class PaddingHelpers
    {
        /// <summary>A size greater than or equal to the size of the most common CPU cache lines.</summary>
        internal const int CACHE_LINE_SIZE = 128;
    }

    /// <summary>Padding structure used to minimize false sharing</summary>
    [StructLayout(LayoutKind.Explicit, Size = PaddingHelpers.CACHE_LINE_SIZE - sizeof(int))]
    internal struct PaddingFor32
    {
    }

    /// <summary>Value type that contains single Int64 value padded on both sides.</summary>
    [StructLayout(LayoutKind.Explicit, Size = 2 * PaddingHelpers.CACHE_LINE_SIZE)]
    internal struct PaddedInt64
    {
        [FieldOffset(PaddingHelpers.CACHE_LINE_SIZE)]
        internal long Value;
    }
}