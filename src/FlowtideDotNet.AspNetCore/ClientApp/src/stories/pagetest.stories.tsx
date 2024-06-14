import type { Meta, StoryObj } from '@storybook/react';
import { within, userEvent, expect } from '@storybook/test';

import Home from '@/app/page';
import exampleData from './exampledata.json'

const meta = {
    title: 'Pages/Index',
    component: Home,
    parameters: {
      // More on how to position stories at: https://storybook.js.org/docs/configure/story-layout
      layout: 'fullscreen',
      mockData: [
        {
          url: 'http://localhost:5186/api/diagnostics',
          method: 'GET',
          status: 200,
          response: exampleData,

        }
      ]
    },
  } satisfies Meta<typeof Home>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {};