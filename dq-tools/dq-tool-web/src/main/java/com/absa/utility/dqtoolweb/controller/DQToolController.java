/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.absa.utility.dqtoolweb.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.absa.utility.dqtoolweb.db.DBInterface;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.Comparator;
import javax.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.RequestParam;

/**
 *
 * @author ABME340
 */
@Controller
public class DQToolController {

    @Autowired
    private DBInterface dBInterface;

    public DQToolController() {
    }

    @RequestMapping(value = {"/", "/index"}, method = RequestMethod.GET)
    public String home(Model model) {
        model.addAttribute("formModel", new FormModel());
        return "index";
    }

    @RequestMapping(value = {"/", "/index"}, method = RequestMethod.POST)
    public String getDaysProduct(@ModelAttribute FormModel formModel) {

        return "redirect:/" + formModel.getFileName();
    }

    @RequestMapping(value = {"/{fileName}"}, method = RequestMethod.GET)
    public String loadPage(@PathVariable String fileName, Model model) {
        List<String> occurences = dBInterface.getFileOccurences(fileName);
        Collections.sort(occurences, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return -1 * o1.compareTo(o2);
            }
        });
        model.addAttribute("dates", occurences);
        model.addAttribute("fileName", fileName);
        return "audit_home";
    }

    @RequestMapping(value = {"/{fileName}/download_bad_records"}, method = RequestMethod.GET)
    public String loadPage(@PathVariable String fileName, @RequestParam String date,
            HttpServletResponse httpServletResponse) {
        //get bad recs
        List<String> badRecords = dBInterface.getBadRecords(fileName, date);
        httpServletResponse.setContentType("application/csv");

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(httpServletResponse.getOutputStream()));) {

            for (String s : badRecords) {
                bw.write(s + "\n");
            }
            bw.flush();
        } catch (IOException ex) {

        }

        return "";
    }

}
