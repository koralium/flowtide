using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace mimalloctests
{
    public unsafe partial class MiMalloc
    {
        private static readonly uint MI_DEBUG = 0;

        private static mi_subproc_t subproc_main = default;
        private static mi_theap_t theap_main = default;
        private static mi_heap_t heap_main = default;

        public static unsafe mi_subproc_t* _mi_subproc_main()
        {
            return (mi_subproc_t*)Unsafe.AsPointer(ref subproc_main);
        }

        private static unsafe mi_theap_t* _mi_theap_main_as_pointer()
        {
            return (mi_theap_t*)Unsafe.AsPointer(ref theap_main);
        }


        //public static unsafe mi_heap_t* mi_heap_main()
        //{
        //    return _mi_subproc_heap_main(_mi_subproc()); // don't use _mi_theap_main() so this call works during process_init
        //}

       
        public static mi_subproc_t* _mi_subproc()
        {
            // should work without doing initialization (as it may be called from `_mi_tld -> mi_tld_alloc ... -> os_alloc -> _mi_subproc()`
            // todo: this will still fail on OS systems where the first access to a thread-local causes allocation.
            //       on such systems we can check for this with the _mi_prim_get_default_theap as those are protected (by being
            //       stored in a TLS slot for example)
            mi_theap_t* theap = _mi_theap_default();
            if (theap == null)
            {
                return _mi_subproc_main();
            }
            else
            {
                return theap->tld->subproc;  // avoid using thread local storage (`thread_tld`)
            }
        }

        //        public static mi_heap_t* _mi_subproc_heap_main(mi_subproc_t* subproc)
        //        {
        //            mi_heap_t* heap = (mi_heap_t*)mi_atomic_load_ptr_relaxed(ref subproc->heap_main.value);
        //            if (mi_likely(heap != null) )
        //            {
        //                return heap;
        //            }
        //            else
        //            {
        //                mi_heap_main_init();
        //                mi_assert_internal(mi_atomic_load_relaxed(&subproc->heap_main) != NULL);
        //                return mi_atomic_load_ptr_relaxed(mi_heap_t, &subproc->heap_main);
        //            }
        //        }

        //        private static void mi_heap_main_init()
        //        {
        //            if (mi_unlikely(heap_main.subproc == null)) 
        //            {
        //                heap_main.subproc = _mi_subproc_main();
        //                heap_main.theaps = _mi_theap_main_as_pointer();

        //                mi_theap_main_init();
        //                mi_subproc_main_init();
        //                mi_tld_main_init();

        //                mi_lock_init(&heap_main.theaps_lock);
        //                mi_lock_init(&heap_main.os_abandoned_pages_lock);
        //                mi_lock_init(&heap_main.arena_pages_lock);
        //            }
        //        }

        static void mi_theap_main_init()
        {
            if (mi_unlikely(theap_main.memid.memkind != mi_memkind_t.MI_MEM_STATIC))
            {
                // theap
                theap_main.memid = _mi_memid_create(mi_memkind_t.MI_MEM_STATIC);
                _mi_random_init(out theap_main.random);

                theap_main.cookie = _mi_theap_random_next(ref theap_main);
                _mi_theap_options_init(ref theap_main);
                _mi_theap_guarded_init(&theap_main);
            }
        }

        public static partial int mi_option_get(mi_option_t option);

        static void _mi_theap_options_init(ref mi_theap_t theap)
        {
            theap.allow_page_reclaim = (mi_option_get(mi_option_page_reclaim_on_free) >= 0);
            theap.allow_page_abandon = (mi_option_get(mi_option_page_full_retain) >= 0);
            theap.page_full_retain = mi_option_get_clamp(mi_option_page_full_retain, -1, 32);
        }
    }
}
