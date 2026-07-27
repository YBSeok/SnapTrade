package com.project.snaptrade.common.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Micrometer {@code process.files.open} is Unix-only ({@code UnixOperatingSystemMXBean}).
 * On Windows, expose the process handle count under the same metric name so Grafana
 * "Open Files" panels work locally.
 */
@Component
public class WindowsProcessFileMetrics implements MeterBinder {

    private static final boolean WINDOWS =
            System.getProperty("os.name", "").toLowerCase().contains("win");

    private static final long CACHE_MILLIS = 2_000L;

    private static final MethodHandle GET_CURRENT_PROCESS;
    private static final MethodHandle GET_PROCESS_HANDLE_COUNT;

    private final AtomicReference<Double> cachedHandles = new AtomicReference<>(Double.NaN);
    private final AtomicLong cachedAt = new AtomicLong(0L);

    static {
        MethodHandle getCurrentProcess = null;
        MethodHandle getProcessHandleCount = null;
        if (WINDOWS) {
            try {
                Linker linker = Linker.nativeLinker();
                SymbolLookup kernel32 = SymbolLookup.libraryLookup("Kernel32", Arena.global());
                getCurrentProcess = linker.downcallHandle(
                        kernel32.find("GetCurrentProcess").orElseThrow(),
                        FunctionDescriptor.of(ValueLayout.ADDRESS)
                );
                getProcessHandleCount = linker.downcallHandle(
                        kernel32.find("GetProcessHandleCount").orElseThrow(),
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS,
                                ValueLayout.ADDRESS
                        )
                );
            } catch (Throwable ignored) {
                // Fall back to PowerShell if native binding is unavailable.
            }
        }
        GET_CURRENT_PROCESS = getCurrentProcess;
        GET_PROCESS_HANDLE_COUNT = getProcessHandleCount;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        if (!WINDOWS) {
            return;
        }

        Gauge.builder("process.files.open", this, WindowsProcessFileMetrics::handleCount)
                .description("Open file descriptors (Windows: process handle count)")
                .baseUnit("files")
                .register(registry);

        // Soft upper bound so dashboards that chart open/max still render.
        Gauge.builder("process.files.max", () -> 16_384.0)
                .description("Approximate max handles used for open-files ratio panels on Windows")
                .baseUnit("files")
                .register(registry);

        Gauge.builder("process.windows.handles", this, WindowsProcessFileMetrics::handleCount)
                .description("Windows process handle count")
                .baseUnit("handles")
                .register(registry);
    }

    private double handleCount() {
        long now = System.currentTimeMillis();
        if (now - cachedAt.get() < CACHE_MILLIS) {
            return cachedHandles.get();
        }

        synchronized (this) {
            if (now - cachedAt.get() < CACHE_MILLIS) {
                return cachedHandles.get();
            }
            double value = queryNativeHandleCount();
            if (Double.isNaN(value)) {
                value = queryPowershellHandleCount();
            }
            cachedHandles.set(value);
            cachedAt.set(now);
            return value;
        }
    }

    private static double queryNativeHandleCount() {
        if (GET_CURRENT_PROCESS == null || GET_PROCESS_HANDLE_COUNT == null) {
            return Double.NaN;
        }
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment process = (MemorySegment) GET_CURRENT_PROCESS.invokeExact();
            MemorySegment countOut = arena.allocate(ValueLayout.JAVA_INT);
            int ok = (int) GET_PROCESS_HANDLE_COUNT.invokeExact(process, countOut);
            if (ok == 0) {
                return Double.NaN;
            }
            return countOut.get(ValueLayout.JAVA_INT, 0);
        } catch (Throwable ignored) {
            return Double.NaN;
        }
    }

    private static double queryPowershellHandleCount() {
        long pid = ProcessHandle.current().pid();
        try {
            Process process = new ProcessBuilder(
                    "powershell.exe",
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    "(Get-Process -Id " + pid + ").HandleCount"
            ).redirectErrorStream(true).start();

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line = reader.readLine();
                if (process.waitFor() == 0 && line != null && !line.isBlank()) {
                    return Double.parseDouble(line.trim());
                }
            }
        } catch (Exception ignored) {
            // Keep series as NaN rather than inventing a value.
        }
        return Double.NaN;
    }
}
