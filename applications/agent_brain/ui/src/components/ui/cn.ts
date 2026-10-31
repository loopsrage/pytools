import { clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: (string | number | boolean | undefined | null)[]) {
  return twMerge(clsx(inputs));
}
