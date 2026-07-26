package com.artivisi.paymentgateway.web.admin;

import com.artivisi.paymentgateway.projection.repository.ChargeProjectionRepository;
import com.artivisi.paymentgateway.projection.repository.PaymentProjectionRepository;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/admin")
public class AdminDashboardController {

    private final ChargeProjectionRepository chargeRepo;
    private final PaymentProjectionRepository paymentRepo;

    public AdminDashboardController(ChargeProjectionRepository chargeRepo, PaymentProjectionRepository paymentRepo) {
        this.chargeRepo = chargeRepo;
        this.paymentRepo = paymentRepo;
    }

    @GetMapping({"", "/", "/dashboard"})
    public String dashboard(Model model) {
        model.addAttribute("charges", chargeRepo.findAll());
        model.addAttribute("payments", paymentRepo.findAll());
        return "admin/dashboard";
    }

    @GetMapping("/charges")
    public String listCharges(Model model) {
        model.addAttribute("charges", chargeRepo.findAll());
        return "admin/charges";
    }

    @GetMapping("/payments")
    public String listPayments(Model model) {
        model.addAttribute("payments", paymentRepo.findAll());
        return "admin/payments";
    }
}
