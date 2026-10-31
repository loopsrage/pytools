import { create } from "zustand";
import { persist, createJSONStorage } from "zustand/middleware";
import type { FavoriteTopic } from "@/types";

interface FavoritesStore {
  favorites: Record<string, FavoriteTopic>;
  isFavorite: (topicId: string) => boolean;
  toggleFavorite: (topicId: string) => void;
  getFavoriteTopics: () => string[];
}

export const useFavoritesStore = create<FavoritesStore>()(
  persist(
    (set, get) => ({
      favorites: {},

      isFavorite: (topicId) => {
        return !!get().favorites[topicId];
      },

      toggleFavorite: (topicId) => {
        set((state) => {
          const favorites = { ...state.favorites };
          if (favorites[topicId]) {
            delete favorites[topicId];
          } else {
            favorites[topicId] = {
              topic_id: topicId,
              pinned_at: new Date().toISOString(),
            };
          }
          return { favorites };
        });
      },

      getFavoriteTopics: () => {
        const { favorites } = get();
        return Object.keys(favorites).sort(
          (a, b) =>
            new Date(favorites[b].pinned_at).getTime() -
            new Date(favorites[a].pinned_at).getTime()
        );
      },
    }),
    {
      name: "agent-brain-favorites-storage",
      storage: createJSONStorage(() => localStorage),
    }
  )
);
