using System;
using System.Collections.Generic;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap
{
    internal abstract class Container : IEquatable<Container>
    {
        public const int MaxSize = 4096; // everything <= is an ArrayContainer
        public const int MaxCapacity = 1 << 16;

        protected internal abstract int Cardinality { get; }

        public abstract int ArraySizeInBytes { get; }

        public bool Equals(Container other)
        {
            if (ReferenceEquals(this, other))
            {
                return true;
            }
            if (ReferenceEquals(null, other))
            {
                return false;
            }
            return EqualsInternal(other);
        }

        protected abstract bool EqualsInternal(Container other);

        public abstract IEnumerator<ushort> GetEnumerator();

        public static Container operator |(Container x, Container y)
        {
            var xArrayContainer = x as ArrayContainer;
            var yArrayContainer = y as ArrayContainer;
            if (xArrayContainer != null && yArrayContainer != null)
            {
                return xArrayContainer | yArrayContainer;
            }
            if (xArrayContainer != null)
            {
                return xArrayContainer | (BitmapContainer)y;
            }
            if (yArrayContainer != null)
            {
                return (BitmapContainer)x | yArrayContainer;
            }
            return (BitmapContainer)x | (BitmapContainer)y;
        }

        public static Container operator &(Container x, Container y)
        {
            var xArrayContainer = x as ArrayContainer;
            var yArrayContainer = y as ArrayContainer;
            if (xArrayContainer != null && yArrayContainer != null)
            {
                return xArrayContainer & yArrayContainer;
            }
            if (xArrayContainer != null)
            {
                return xArrayContainer & (BitmapContainer)y;
            }
            if (yArrayContainer != null)
            {
                return (BitmapContainer)x & yArrayContainer;
            }
            return (BitmapContainer)x & (BitmapContainer)y;
        }

        public static Container operator ^(Container x, Container y)
        {
            var xArrayContainer = x as ArrayContainer;
            var yArrayContainer = y as ArrayContainer;
            if (xArrayContainer != null && yArrayContainer != null)
            {
                return xArrayContainer ^ yArrayContainer;
            }
            if (xArrayContainer != null)
            {
                return xArrayContainer ^ (BitmapContainer)y;
            }
            if (yArrayContainer != null)
            {
                return (BitmapContainer)x ^ yArrayContainer;
            }
            return (BitmapContainer)x ^ (BitmapContainer)y;
        }

        public static Container operator ~(Container x)
        {
            var xArrayContainer = x as ArrayContainer;
            return xArrayContainer != null ? ~xArrayContainer : ~(BitmapContainer)x;
        }

        public static Container AndNot(Container x, Container y)
        {
            var xArrayContainer = x as ArrayContainer;
            var yArrayContainer = y as ArrayContainer;
            if (xArrayContainer != null && yArrayContainer != null)
            {
                return ArrayContainer.AndNot(xArrayContainer, yArrayContainer);
            }
            if (xArrayContainer != null)
            {
                return ArrayContainer.AndNot(xArrayContainer, (BitmapContainer)y);
            }
            if (yArrayContainer != null)
            {
                return BitmapContainer.AndNot((BitmapContainer)x, yArrayContainer);
            }
            return BitmapContainer.AndNot((BitmapContainer)x, (BitmapContainer)y);
        }
    }
}