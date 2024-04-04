"use client"

import { Disclosure } from '@headlessui/react'
import React from 'react'

export default function Header() {
  return (
      <div className="min-h-full">
        <Disclosure as="nav" className="bg-gray-800">
          {({ open }) => (
              <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
                <div className="flex h-16 items-center justify-between">
                  <div className="flex items-center">
                    <div className="flex-shrink-0">
                        <h1 className="text-3xl font-bold tracking-tight text-gray-100">Flowtide.NET</h1>
                    </div>
                    <div className="hidden md:block">
                      <div className="ml-10 flex items-baseline space-x-4">
                      </div>
                    </div>
                  </div>
                  <div className="hidden md:block">
                    <div className="ml-4 flex items-center md:ml-6">
                    </div>
                  </div>
                </div>
              </div>
          )}
        </Disclosure>
      </div>
  )
}
