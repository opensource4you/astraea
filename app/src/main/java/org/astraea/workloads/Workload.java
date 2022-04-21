package org.astraea.workloads;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.UnixStyleUsageFormatter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.workloads.annotations.NamedArg;

public class Workload {

  private static final List<Class<?>> classes =
      List.of(ProducerWorkloads.class, ConsumerWorkloads.class);
  private static final Map<Class<?>, List<Method>> workloadMethods =
      classes.stream()
          .flatMap(x -> Arrays.stream(x.getDeclaredMethods()))
          .filter(
              x ->
                  (x.getReturnType() == ConsumerWorkload.class)
                      || (x.getReturnType() == ProducerWorkload.class))
          .collect(Collectors.groupingBy(Method::getDeclaringClass, Collectors.toList()));

  public static void main(String[] args) {
    var argument = org.astraea.argument.Argument.parse(new Argument(), args);

    if (argument.listWorkloads) {
      help();
      return;
    }

    if (!argument.workloadFile.equals("")) {
      executeWorkloadFile(argument.workloadFile);
    } else if (!argument.adhocWorkload.isEmpty()) {
      final WorkloadTask workloadTask =
          parseWorkload(argument.adhocWorkload.toArray(new String[0]));
      System.out.println(
          "Execute "
              + workloadTask.workloadCall.getDeclaringClass().getName()
              + "#"
              + workloadTask.workloadCall.getName());
      ((Runnable) workloadTask.invoke()).run();
    } else {
      StringBuilder sb = new StringBuilder();
      final JCommander build = JCommander.newBuilder().addObject(argument).build();
      build.setUsageFormatter(new UnixStyleUsageFormatter(build));
      build.getUsageFormatter().usage(sb);
      throw new IllegalArgumentException("No operation specified\n" + sb);
    }
  }

  public static void executeWorkloadFile(String workloadFilePath) {
    try {
      final var lines = Files.readAllLines(Path.of(workloadFilePath));
      final var workloadTasks =
          lines.stream().map(line -> parseWorkload(line.split(" "))).collect(Collectors.toList());
      if (workloadTasks.isEmpty())
        throw new IllegalArgumentException("The given workload file is empty.");
      workloadTasks.forEach(
          workloadTask ->
              System.out.println(
                  "Execute "
                      + workloadTask.workloadCall.getDeclaringClass().getName()
                      + "#"
                      + workloadTask.workloadCall.getName()));
      final var threads =
          workloadTasks.stream()
              .map(x -> new Thread((Runnable) x.invoke()))
              .collect(Collectors.toList());
      threads.forEach(Thread::start);
      for (Thread thread : threads) {
        thread.join();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static class Argument {
    @com.beust.jcommander.Parameter(
        names = {"--run-workload"},
        variableArity = true)
    public List<String> adhocWorkload = List.of();

    @com.beust.jcommander.Parameter(names = {"--list-workloads"})
    public boolean listWorkloads = false;

    @com.beust.jcommander.Parameter(names = {"--workload-file"})
    public String workloadFile = "";
  }

  private static WorkloadTask parseWorkload(String[] args) {

    final var pattern = Pattern.compile("(?<WorkloadType>.{1,255})#(?<WorkloadName>.{1,255})");
    final var workloadMatch = pattern.matcher(args[0]);
    if (!workloadMatch.matches())
      throw new IllegalArgumentException("Illegal workload type" + args[0]);
    final var workloadTypeName = workloadMatch.group("WorkloadType");
    final var workloadMethodName = workloadMatch.group("WorkloadName");
    final var arguments = Arrays.copyOfRange(args, 1, args.length);

    // find the target workload method
    final var workloadMethod =
        workloadMethods.entrySet().stream()
            .filter(
                entry ->
                    entry.getKey().getSimpleName().equals(workloadTypeName)
                        || entry.getKey().getName().equals(workloadTypeName))
            .flatMap(aClass -> aClass.getValue().stream())
            .filter(method -> method.getName().equals(workloadMethodName))
            .findFirst()
            .orElseThrow();

    // attempts to type-ify the string argument
    final var workloadParameters = workloadMethod.getParameters();
    final var typedArgument =
        IntStream.range(0, workloadParameters.length)
            .mapToObj(index -> Map.entry(workloadParameters[index], arguments[index]))
            .map(entry -> casting(entry.getKey(), entry.getValue()))
            .toArray();

    return new WorkloadTask(workloadMethod, typedArgument);
  }

  private static void help() {
    System.out.println("[Supported workload methods]");

    var newline = System.lineSeparator();
    for (Class<?> aClass : classes) {
      for (Method workloadMethod : workloadMethods.get(aClass)) {
        StringBuilder sb = new StringBuilder();
        sb.append(" ")
            .append(aClass.getSimpleName())
            .append("#")
            .append(workloadMethod.getName())
            .append("(")
            .append(newline);
        for (Parameter parameter : workloadMethod.getParameters()) {
          var nameAnnotation = parameter.getAnnotation(NamedArg.class);
          var hasNameAnnotation = nameAnnotation != null;
          var parameterName = hasNameAnnotation ? nameAnnotation.name() : parameter.getName();
          sb.append("    ")
              .append(parameterName)
              .append(" : ")
              .append(parameter.getType().getSimpleName());
          if (hasNameAnnotation && !nameAnnotation.description().equals(""))
            sb.append("   // ").append(nameAnnotation.description());
          sb.append(newline);
        }
        sb.append(" )");
        System.out.println(sb);
      }
    }
  }

  public static Object casting(Parameter parameter, String string) {
    if (String.class == parameter.getType()) return string;
    else if (int.class == parameter.getType()) return Integer.parseInt(string);
    else if (long.class == parameter.getType()) return Long.parseLong(string);
    else if (boolean.class == parameter.getType()) return Boolean.parseBoolean(string);
    else if (short.class == parameter.getType()) return Short.parseShort(string);
    else if (double.class == parameter.getType()) return Double.parseDouble(string);
    else return null;
  }

  private static class WorkloadTask {
    public final Method workloadCall;
    public final Object[] typedArgument;

    private WorkloadTask(Method workloadCall, Object[] typedArgument) {
      this.workloadCall = workloadCall;
      this.typedArgument = typedArgument;
    }

    public Object invoke() {
      try {
        return workloadCall.invoke(null, typedArgument);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
