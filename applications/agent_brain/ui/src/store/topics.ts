import { create } from "zustand";
import type { Topic } from "@/types";

interface TopicState {
  selectedTopicId: string | null;
  setSelectedTopic: (id: string | null) => void;
  topics: Topic[];
  setTopics: (topics: Topic[]) => void;
}

export const useTopicState = create<TopicState>((set) => ({
  selectedTopicId: null,
  topics: [],
  setSelectedTopic: (id) => set({ selectedTopicId: id }),
  setTopics: (topics) => set({ topics }),
}));
