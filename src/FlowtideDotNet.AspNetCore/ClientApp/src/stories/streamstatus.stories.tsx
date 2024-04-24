import type { Meta, StoryObj } from '@storybook/react';
import { within, userEvent, expect } from '@storybook/test';

import { StreamStatus } from '@/app/components/streamstatus';

const meta = {
    title: 'Components/StreamStatus',
    component: StreamStatus,
  } satisfies Meta<typeof StreamStatus>;

export default meta;
type Story = StoryObj<typeof meta>;

export const NotStarted: Story = {
    args: {
        status: "Notstarted"
    }
};

export const Running: Story = {
    args: {
        status: "running"
    }
};

export const Starting: Story = {
    args: {
        status: "starting"
    }
};

export const Failure: Story = {
    args: {
        status: "failure"
    }
};

export const Unknown: Story = {
    args: {
        status: "Unknown"
    }
};

export const AlignRight: Story = {
    args: {
        status: "running",
        align: "right"
    }
};

