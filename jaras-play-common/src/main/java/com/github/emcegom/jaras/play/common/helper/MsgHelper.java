package com.github.emcegom.jaras.play.common.helper;

public class MsgHelper {
    /**
     * 格式化消息模板
     * @param template 模板字符串，例如 "User {} not found in {}"
     * @param args 参数，会按顺序替换 {}
     * @return 格式化后的字符串
     */
    public static String m(String template, Object... args) {
        if (template == null) {
            return null;
        }
        if (args == null || args.length == 0) {
            return template;
        }

        StringBuilder sb = new StringBuilder();
        int argIndex = 0;
        int start = 0;
        int placeholderIndex;

        while ((placeholderIndex = template.indexOf("{}", start)) != -1 && argIndex < args.length) {
            sb.append(template, start, placeholderIndex);
            sb.append(args[argIndex] == null ? "null" : args[argIndex].toString());
            start = placeholderIndex + 2;
            argIndex++;
        }

        // 添加剩余模板内容
        sb.append(template.substring(start));

        return sb.toString();
    }
}
