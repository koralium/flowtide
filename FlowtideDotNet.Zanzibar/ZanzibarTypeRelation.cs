namespace FlowtideDotNet.Zanzibar
{
    public abstract class ZanzibarTypeRelation
    {
        public ZanzibarTypeRelation()
        {
        }

        internal abstract string DebuggerDisplay { get; }

        public abstract T Accept<T, TState>(ZanzibarTypeRelationVisitor<T, TState> visitor, TState state);
    }
}
