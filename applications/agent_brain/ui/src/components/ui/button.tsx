import * as React from "react";
import { Slot } from "@radix-ui/react-slot";
import { cn } from "./cn";

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "default" | "destructive" | "outline" | "secondary" | "ghost" | "link";
  size?: "default" | "sm" | "lg" | "icon";
  asChild?: boolean;
}

const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant = "default", size = "default", asChild = false, ...props }, ref) => {
    const Comp = asChild ? Slot : "button";

    const variants = {
      default: "bg-primary text-primary-foreground px-4 py-2 rounded-md font-medium shadow-sm hover:bg-primary/90 transition-colors",
      destructive: "bg-destructive text-destructive-foreground px-4 py-2 rounded-md font-medium shadow-sm hover:bg-destructive/90 transition-colors",
      outline: "border border-input bg-background hover:bg-accent hover:text-accent-foreground px-4 py-2 rounded-md font-medium transition-colors",
      secondary: "bg-secondary text-secondary-foreground px-4 py-2 rounded-md font-medium shadow-sm hover:bg-secondary/80 transition-colors",
      ghost: "hover:bg-accent hover:text-accent-foreground px-4 py-2 rounded-md font-medium transition-colors",
      link: "text-primary underline-offset-4 hover:underline font-medium",
    };

    const sizes = {
      default: "h-10 px-4 py-2",
      sm: "h-9 rounded-md px-3",
      lg: "h-11 rounded-md px-8",
      icon: "h-10 w-10",
    };

    return (
      <Comp
        ref={ref}
        className={cn(
          "inline-flex items-center justify-center whitespace-nowrap rounded-md text-sm font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50",
          variants[variant],
          sizes[size],
          className
        )}
        {...props}
      />
    );
  }
);
Button.displayName = "Button";

export { Button };
