import { create } from "zustand";
import { persist, createJSONStorage } from "zustand/middleware";

export interface ResumeState {
  resume: string | null;
  setResume: (path: string) => void;
  clearResume: () => void;
}

export const useResumeStore = create<ResumeState>()(
  persist(
    (set) => ({
      resume: null,
      setResume: (path) => set({ resume: path }),
      clearResume: () => set({ resume: null }),
    }),
    {
      name: "agent-brain-resume-storage",
      storage: createJSONStorage(() => localStorage),
    }
  )
);
